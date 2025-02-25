/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.parse;

import com.alibaba.fastjson.JSONObject;
import org.opengauss.tool.config.parse.ParseConfig;
import org.opengauss.tool.dispatcher.WorkTask;
import org.opengauss.tool.parse.object.DatabaseTypeEnum;
import org.opengauss.tool.parse.object.OriginPacket;
import org.opengauss.tool.parse.object.PacketData;
import org.opengauss.tool.parse.object.ProtocolConstant;
import org.opengauss.tool.parse.object.SessionInfo;
import org.opengauss.tool.parse.object.SqlInfo;
import org.opengauss.tool.parse.object.SelectResult;
import org.opengauss.tool.utils.CommonParser;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.utils.DatabaseOperator;
import org.opengauss.tool.utils.FileOperator;
import org.opengauss.tool.utils.FileUtils;
import org.opengauss.tool.utils.ThreadExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Description: Parse packet file
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/05
 */
public class ParseTask extends WorkTask {
    private static final ConcurrentMap<String, ParseThread> THREAD_MAP = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseTask.class);
    private static final String PCAP_SUFFIX = ".pcap";
    private static final DateTimeFormatter TIME_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss.SSS");
    private static final BlockingQueue<SelectResult> RESULT_QUEUE = new LinkedBlockingQueue<>();
    private static final int BYTE_CONVERSION_RATIO = 1024 * 1024;
    private static final String PROCESS_FILE_NAME = "parse-process.txt";

    private final ParseConfig config;
    private final ThreadPoolExecutor threadPool;
    private final BlockingQueue<OriginPacket> packetQueue;
    private final AtomicLong packetId;
    private final AtomicBoolean isBlock;
    private final AtomicBoolean isReadFilesFinished;
    private final AtomicBoolean isParseFinished;
    private final AtomicBoolean isCommitSqlFinished;
    private final Set<SessionInfo> sessionInfoSet;
    private final DatabaseTypeEnum databaseTypeEnum;
    private DatabaseOperator opengaussOperator;
    private FileOperator fileOperator;
    private LocalDateTime startTime;
    private int fileId = 1;
    private String processPath;

    /**
     * Constructor
     *
     * @param config ParseConfig the config
     */
    public ParseTask(ParseConfig config) {
        this.config = config;
        this.threadPool = new ThreadPoolExecutor(5, 5, 100, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5));
        this.packetQueue = new LinkedBlockingQueue<>();
        this.packetId = new AtomicLong();
        this.isBlock = new AtomicBoolean(false);
        this.isReadFilesFinished = new AtomicBoolean(false);
        this.isParseFinished = new AtomicBoolean(false);
        this.isCommitSqlFinished = new AtomicBoolean(false);
        this.sessionInfoSet = new HashSet<>();
        this.databaseTypeEnum = DatabaseTypeEnum.fromTypeName(config.getDatabaseServerType());
        initStorage();
    }

    /**
     * add select result to queue
     *
     * @param result select result
     */
    public static void addResultToQueue(SelectResult result) {
        RESULT_QUEUE.add(result);
    }

    private void initStorage() {
        this.processPath = FileUtils.getJarPath() + File.separator + PROCESS_FILE_NAME;
        FileUtils.createFile(processPath);
        if (config.getOpengaussConfig() != null) {
            opengaussOperator = new DatabaseOperator(config.getOpengaussConfig(), "openGauss");
            opengaussOperator.initStorage(config.getOpengaussConfig(), true, config.isDropPreviousSql());
        } else {
            fileOperator = new FileOperator(config.getFileConfig());
            FileOperator.createPath(config.getFileConfig().getFilePath(), config.getFileConfig().getFileName());
        }
    }

    @Override
    public void start() {
        threadPool.execute(this::readPcapFile);
        threadPool.execute(this::distributeData);
        threadPool.execute(this::mergeSql);
        if (config.getResultFileConfig().isParseResult()) {
            threadPool.execute(this::saveSelectResult);
        }
    }

    private void readPcapFile() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        long startTimeMillis = System.currentTimeMillis();
        File dir = new File(config.getPacketFilePath());
        startTime = LocalDateTime.now();
        int point = 0;
        while (true) {
            List<File> files = getValidPacketFiles(dir);
            if (files.isEmpty() && !FileUtils.isFinished(config.getPacketFilePath(), "endFile")) {
                long readTime = (System.currentTimeMillis() - startTimeMillis) / 60000;
                if (config.getParseMaxTime() > 0 && readTime >= config.getParseMaxTime()) {
                    break;
                }
                sleep(1000);
                continue;
            }

            if (FileUtils.isFinished(config.getPacketFilePath(), "endFile")) {
                if (config.isDropTcpdumpFile()) {
                    parseAndDeleteFile(files, files.size());
                } else {
                    parseFile(files, files.size(), point);
                }
                break;
            }

            if (config.isDropTcpdumpFile()) {
                parseAndDeleteFile(files, files.size() - 1);
            } else {
                parseFile(files, files.size() - 1, point);
                point = files.size() - 1;
            }
            int currentFileCount = getValidPacketFiles(dir).size();
            if (currentFileCount == files.size()
                    && !FileUtils.isFinished(config.getPacketFilePath(), "endFile")) {
                long readTime = (System.currentTimeMillis() - startTimeMillis) / 60000;
                if (config.getParseMaxTime() > 0 && readTime >= config.getParseMaxTime()) {
                    break;
                }
            }
        }
        isReadFilesFinished.set(true);
        LOGGER.info("All packet files have been loaded.");
    }

    private void parseAndDeleteFile(List<File> files, int size) {
        for (int order = 0; order < size; order++) {
            splitPacket(files.get(order));
            files.get(order).delete();
        }
    }

    private void parseFile(List<File> files, int size, int point) {
        for (int order = point; order < size; order++) {
            splitPacket(files.get(order));
        }
    }

    private void splitPacket(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] pcapHeader = new byte[ProtocolConstant.PCAP_HEADER_LENGTH];
            fis.read(pcapHeader);
            int id = 0;
            while (true) {
                if (packetQueue.size() > config.getQueueSizeLimit()) {
                    sleep(1000);
                    continue;
                }
                byte[] packetHeader = new byte[ProtocolConstant.PACKET_HEADER_LENGTH];
                int flag = fis.read(packetHeader);
                if (flag == -1) {
                    break;
                }
                id++;
                long timestamp = CommonParser.parseTimestamp(packetHeader);
                String hexCap = CommonParser.parseByLittleEndian(packetHeader, 8, 12);
                int cap = Integer.parseInt(hexCap, 16);
                String hexLen = CommonParser.parseByLittleEndian(packetHeader, 12, 16);
                int len = Integer.parseInt(hexLen, 16);
                byte[] dataFlame = new byte[len];
                fis.read(dataFlame);
                if (len <= ProtocolConstant.ETHERNET_HEADER_LENGTH) {
                    continue;
                }
                packetQueue.offer(new OriginPacket(file.getName(), id, dataFlame, timestamp));
            }
        } catch (IOException e) {
            LOGGER.error("IOException occurred while reading the file {}, error message is: {}.",
                    file.getName(), e.getMessage());
        }
        LOGGER.info("Have read the file {} completed.", file.getName());
    }

    private List<File> getValidPacketFiles(File dir) {
        File[] files = dir.listFiles();
        if (files == null) {
            return new ArrayList<>(0);
        }
        List<File> validPcapFile = new ArrayList<>();
        for (File file : files) {
            if (matchingPcap(file.getName())) {
                validPcapFile.add(file);
            }
        }
        validPcapFile.sort(Comparator.comparingInt(o -> getFileIndex(o.getName())));
        return validPcapFile;
    }

    private int getFileIndex(String name) {
        if (name.endsWith(PCAP_SUFFIX)) {
            return 0;
        }
        return Integer.parseInt(name.split(PCAP_SUFFIX)[1]);
    }

    private boolean matchingPcap(String name) {
        return name.matches(".*\\.pcap[0-9]+$") || name.endsWith(PCAP_SUFFIX);
    }

    private void distributeData() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        OriginPacket originPacket;
        byte[] packet;
        int headerLength;
        int sourcePort;
        int destinationPort;
        int skipLength;
        String sourceIp;
        String destinationIp;
        while (true) {
            if (isBlock.get()) {
                continue;
            }
            originPacket = packetQueue.poll();
            if (originPacket == null || originPacket.getOriginData().length == 0) {
                if (isReadFilesFinished.get()) {
                    break;
                }
                continue;
            }
            packet = originPacket.getOriginData();
            packetId.incrementAndGet();
            if (ProtocolConstant.IPV4.equals(originPacket.getIpType())) {
                headerLength = CommonParser.parseIntByLittleEndian(packet, 46, 47) / 4;
                skipLength = ProtocolConstant.ETHERNET_HEADER_LENGTH + ProtocolConstant.IPV4_HEADER_LENGTH
                        + headerLength;
                if (packet.length <= skipLength) {
                    continue;
                }
                sourceIp = parseIPV4Address(packet, 26, 30);
                destinationIp = parseIPV4Address(packet, 30, 34);
                sourcePort = CommonParser.parseIntByBigEndian(packet, 34, 36);
                destinationPort = CommonParser.parseIntByBigEndian(packet, 36, 38);
            } else {
                headerLength = CommonParser.parseIntByLittleEndian(packet, 66, 67) / 4;
                skipLength = ProtocolConstant.ETHERNET_HEADER_LENGTH + ProtocolConstant.IPV6_HEADER_LENGTH
                        + headerLength;
                if (packet.length <= skipLength) {
                    continue;
                }
                sourceIp = parseIPV6Address(packet, 22, 38);
                destinationIp = parseIPV6Address(packet, 38, 54);
                sourcePort = CommonParser.parseIntByBigEndian(packet, 54, 56);
                destinationPort = CommonParser.parseIntByBigEndian(packet, 56, 58);
            }
            PacketData packetData = new PacketData(packetId.get(), identifyPacketType(sourceIp, sourcePort));
            packetData.setOriginInfo(originPacket);
            packetData.build(sourceIp, sourcePort, destinationIp, destinationPort);
            packetData.clonePacketData(packet, skipLength);
            distribute(packetData);
        }
        stop();
    }

    private void distribute(PacketData packetData) {
        String clientId = packetData.getClientId();
        if (THREAD_MAP.containsKey(clientId)) {
            THREAD_MAP.get(clientId).addDataToQueue(packetData);
        } else {
            ParseThread parseThread = databaseTypeEnum.getSuitableProtocolParser(clientId);
            parseThread.addDataToQueue(packetData);
            parseThread.setFileConfig(config.getResultFileConfig());
            parseThread.start();
            THREAD_MAP.put(clientId, parseThread);
        }
    }

    private void informSubThread() {
        for (Map.Entry<String, ParseThread> map : THREAD_MAP.entrySet()) {
            map.getValue().setIsDistributeFinished(true);
        }
        LOGGER.info("All network packets have been distributed.");
    }

    private void waitParseFinish() {
        while (!isParseFinished.get()) {
            isParseFinished.set(isParsePacketFinished());
            sleep(1000);
        }
        LOGGER.info("All network packets have been parsed.");
    }

    private void waitCommitSqlFinish() {
        while (!isCommitSqlFinished.get()) {
            isCommitSqlFinished.set(isCommitSqlFinished());
            sleep(1000);
        }
    }

    private boolean isParsePacketFinished() {
        for (Map.Entry<String, ParseThread> map : THREAD_MAP.entrySet()) {
            if (!map.getValue().isParseFinished()) {
                return false;
            }
        }
        return true;
    }

    private boolean isCommitSqlFinished() {
        for (Map.Entry<String, ParseThread> map : THREAD_MAP.entrySet()) {
            if (!map.getValue().getSqlQueue().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void mergeSql() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        long flag = 0L;
        List<SqlInfo> sqlList = new ArrayList<>();
        BlockingQueue<ParseThread> parseThreadQueue = new LinkedBlockingQueue<>();
        BlockingQueue<ParseThread> commitThreadQueue = new LinkedBlockingQueue<>();
        do {
            if (packetId.get() - flag >= config.getPacketBatchSize() || isParseFinished.get()) {
                isBlock.set(true);
                flag = packetId.get();
                parseThreadQueue.addAll(THREAD_MAP.values());
                long minSqlId = Long.MAX_VALUE;
                while (!parseThreadQueue.isEmpty()) {
                    ParseThread thread = parseThreadQueue.poll();
                    if (!thread.getPacketQueue().isEmpty()) {
                        parseThreadQueue.offer(thread);
                        continue;
                    }
                    commitThreadQueue.offer(thread);
                    SqlInfo incompleteSql = thread.getIncompleteSql();
                    if (incompleteSql == null) {
                        continue;
                    }
                    if (incompleteSql.getEndTime() != 0) {
                        thread.addSqLToQueue();
                    } else {
                        if (incompleteSql.getSqlId() < minSqlId) {
                            minSqlId = incompleteSql.getSqlId();
                        }
                    }
                }
                mergeThreadSql(sqlList, commitThreadQueue, minSqlId);
                if (sessionInfoSet.size() > config.getPacketBatchSize()) {
                    commitSessionInformation();
                }
                isBlock.set(false);
                sqlList.sort((o1, o2) -> (int) (o1.getSqlId() - o2.getSqlId()));
                storageSql(sqlList, true);
                sqlList.clear();
            }
        } while (!isCommitSqlFinished.get());
        storageFinishedSql();
        addIndexToTable();
        LOGGER.info("All sql information have been committed.");
        stat();
        threadPool.shutdown();
    }

    private void saveSelectResult() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        String resultFilePath = config.getResultFileConfig().getSelectResultPath() + File.separator
                + config.getResultFileConfig().getResultFileName() + "-" + fileId + ".json";
        File resultFile = new File(resultFilePath);
        if (!resultFile.exists()) {
            try {
                resultFile.createNewFile();
            } catch (IOException e) {
                LOGGER.error("IOException occurred while creating result file {}, error message is: {}.",
                        resultFilePath, e.getMessage());
            }
            saveSelectResult();
        } else if (resultFile.length() >= (long) config.getResultFileConfig().getResultFileSize()
                * BYTE_CONVERSION_RATIO) {
            fileId++;
            saveSelectResult();
        } else {
            writeResultToFile(resultFilePath);
        }
    }

    private void writeResultToFile(String resultFilePath) {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(resultFilePath, true), StandardCharsets.UTF_8))) {
            while (!RESULT_QUEUE.isEmpty() || !isParseFinished.get()) {
                SelectResult sr = RESULT_QUEUE.poll();
                if (sr == null) {
                    sleep(1000);
                    continue;
                }
                JSONObject json = new JSONObject();
                json.fluentPut("sqlPacketId", sr.getPreviousSql().getPacketId())
                        .fluentPut("sql", sr.getPreviousSql().getSql())
                        .fluentPut("param", sr.getPreviousSql().getParameterList())
                        .fluentPut("packetId", sr.getPacketId())
                        .fluentPut("rowCount", sr.getRowCount())
                        .fluentPut("data", sr.getDataList());
                writer.write(json.toString());
                writer.flush();
                writer.newLine();
            }
        } catch (IOException e) {
            LOGGER.error("Error occurred during write result to file, error message is: {}",
                    e.getMessage());
        }
    }

    private void storageFinishedSql() {
        List<SqlInfo> sqlList = new ArrayList<>();
        SqlInfo endInfo = new SqlInfo();
        endInfo.setSql("finished");
        sqlList.add(endInfo);
        storageSql(sqlList, false);
        if (config.getStorageMode().equals(ConfigReader.JSON)) {
            fileOperator.sendFinishedFlag();
        }
    }

    private void mergeThreadSql(List<SqlInfo> sqlList, BlockingQueue<ParseThread> parseThreadQueue, long minSqlId) {
        ParseThread parseThread;
        while (!parseThreadQueue.isEmpty()) {
            parseThread = parseThreadQueue.poll();
            if (parseThread.isParseFinished()) {
                sqlList.addAll(parseThread.getSqlQueue());
                if (parseThread.getSchema() != null) {
                    sessionInfoSet.add(new SessionInfo(parseThread.getSessionId(), parseThread.getUsername(),
                            parseThread.getSchema()));
                }
                THREAD_MAP.remove(parseThread.getSessionId());
            } else {
                addSqlToList(parseThread.getSqlQueue(), sqlList, minSqlId);
            }
        }
    }

    private void addSqlToList(BlockingQueue<SqlInfo> sqlQueue, List<SqlInfo> sqlList, long minSqlId) {
        while (!sqlQueue.isEmpty()) {
            SqlInfo sql = sqlQueue.peek();
            if (sql.getSqlId() < minSqlId) {
                sqlList.add(sql);
                sqlQueue.poll();
            } else {
                break;
            }
        }
    }

    @Override
    public void stat() {
        LocalDateTime curr = LocalDateTime.now();
        String table = "       ";
        String line = System.lineSeparator();
        String start = TIME_PATTERN.format(startTime);
        String current = TIME_PATTERN.format(curr);
        long duration = curr.toEpochSecond(ZoneOffset.UTC) - startTime.toEpochSecond(ZoneOffset.UTC);
        String res = String.format("%s Start Time: %s%s%s End Time: %s%s%s Duration: %s seconds%s%s SQL Count: %s",
                table, start, line, table, current, line, table, duration, line, table, opengaussOperator == null
                        ? fileOperator.getSqlId() - 1 : opengaussOperator.getSqlId() - 1);
        LOGGER.info("Parse finished, the statistical results are as follows:{}{}", line, res);
    }

    private String identifyPacketType(String sourceIp, int sourcePort) {
        if (sourceIp.equals(config.getDatabaseServerIp()) && sourcePort == config.getDatabaseServerPort()) {
            return ProtocolConstant.RESPONSE;
        }
        return ProtocolConstant.REQUEST;
    }

    private String parseIPV4Address(byte[] packet, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(CommonParser.parseIntByLittleEndian(packet, i, i + 1)).append(".");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private String parseIPV6Address(byte[] packet, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i += 2) {
            sb.append(CommonParser.parseByBigEndian(packet, i, i + 2)).append(":");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private void storageSql(List<SqlInfo> sqlList, boolean shouldRefreshProcess) {
        if (sqlList.isEmpty()) {
            return;
        }
        if (config.getStorageMode().equals(ConfigReader.DB)) {
            opengaussOperator.refreshConnection();
            opengaussOperator.insertSqlToDatabase(sqlList, true);
            statParseProcess(opengaussOperator.getSqlId(), shouldRefreshProcess);
        } else {
            fileOperator.writeSqlToFile(sqlList, true);
            statParseProcess(fileOperator.getSqlId(), shouldRefreshProcess);
            LOGGER.info("Commit {} sql to file.", sqlList.size());
        }
    }

    private void statParseProcess(long sqlId, boolean shouldRefreshProcess) {
        if (shouldRefreshProcess) {
            FileUtils.write2File(String.valueOf(sqlId), processPath);
        }
    }

    private void stop() {
        informSubThread();
        waitParseFinish();
        waitCommitSqlFinish();
        mergeFinalSession();
        commitSessionInformation();
    }

    private void mergeFinalSession() {
        for (Map.Entry<String, ParseThread> map : THREAD_MAP.entrySet()) {
            String sessionId = map.getKey();
            String username = map.getValue().getUsername();
            if (username == null) {
                continue;
            }
            String schema = map.getValue().getSchema();
            sessionInfoSet.add(new SessionInfo(sessionId, username, schema));
        }
    }

    private void commitSessionInformation() {
        if (sessionInfoSet.isEmpty()) {
            return;
        }
        if (opengaussOperator != null) {
            opengaussOperator.insertSessionToDb(sessionInfoSet);
        } else {
            fileOperator.writeSessionToFile(sessionInfoSet);
        }
        sessionInfoSet.clear();
    }

    private void addIndexToTable() {
        if (opengaussOperator == null) {
            return;
        }
        opengaussOperator.addIndexToTable(config.getOpengaussConfig().getTableName() + "_paras");
        opengaussOperator.close();
    }
}