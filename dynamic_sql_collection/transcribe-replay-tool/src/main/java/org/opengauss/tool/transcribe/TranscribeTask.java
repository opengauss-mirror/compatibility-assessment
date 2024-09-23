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

package org.opengauss.tool.transcribe;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.opengauss.tool.config.transcribe.TranscribeConfig;
import org.opengauss.tool.dispatcher.WorkTask;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.utils.ConnectionFactory;
import org.opengauss.tool.utils.DatabaseOperator;
import org.opengauss.tool.utils.FileOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description: Transcribe task
 *
 * @author wangzhengyuan
 * @since 2024/05/25
 **/
public class TranscribeTask extends WorkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(TranscribeTask.class);
    private static final String PCAP_SUFFIX = ".pcap";
    private static final int SECOND_OF_MINUTE = 60;
    /**
     * Time formatter
     */
    protected static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss.SSS");

    /**
     * openGauss operator
     */
    protected DatabaseOperator opengaussOperator;

    /**
     * file operator
     */
    protected FileOperator fileOperator;

    /**
     * transcribe config
     */
    protected TranscribeConfig config;

    /**
     * thread pool
     */
    protected ThreadPoolExecutor threadPool;

    /**
     * Start time
     */
    protected LocalDateTime startTime;
    private Map<String, String> parameterMap;
    private CommandProcess process;
    private Thread tcpdump;
    private AtomicBoolean isCatchFinished;
    private AtomicBoolean isNormalStop;
    private long timer;
    private int retryCount;
    private int fileCount;

    /**
     * Constructor
     *
     * @param config TranscribeConfig the config
     */
    public TranscribeTask(TranscribeConfig config) {
        this.config = config;
        this.threadPool = new ThreadPoolExecutor(1, 1, 100, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1));
        this.parameterMap = new HashMap<>();
        this.isCatchFinished = new AtomicBoolean(false);
        this.isNormalStop = new AtomicBoolean(true);
        initStorage();
    }

    /**
     * Initialize storage
     */
    protected void initStorage() {
        if (config.isWriteToFile()) {
            FileOperator.createPath(config.getFileConfig().getFilePath());
            fileOperator = new FileOperator(config.getFileConfig());
        } else {
            opengaussOperator = new DatabaseOperator(config.getOpengaussConfig(), ConnectionFactory.OPENGAUSS);
            opengaussOperator.initStorage(config.getOpengaussConfig(), false, config.isDropPreviousSql());
        }
    }

    @Override
    public void start() {
        if (config.shouldCheckResource()) {
            monitorDisk();
            monitorMemory();
            monitorCpu();
            if (isNormalStop.get()) {
                ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((r)
                        -> new Thread(r, "Resource Monitoring Thread"));
                scheduledExecutorService.scheduleAtFixedRate(() -> monitorSystemResource(scheduledExecutorService),
                        10, 10, TimeUnit.SECONDS);
            }
        }
        capturePacket();
        if (config.shouldSendFile()) {
            threadPool.execute(this::arrangeFiles);
        }
    }

    /**
     * Capture packet
     */
    protected void capturePacket() {
        if (!isNormalStop.get()) {
            return;
        }
        initCommandParameters();
        String command = buildCommand();
        process = new CommandProcess(command);
        tcpdump = new Thread(process, "Transcribe Thread");
        tcpdump.start();
        LOGGER.info("Start to capture network packet or sql...");
        startTime = LocalDateTime.now();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((r)
                -> new Thread(r, "Heartbeat Thread"));
        scheduledExecutorService.scheduleAtFixedRate(()
                -> heartbeat(scheduledExecutorService), 1, 1, TimeUnit.SECONDS);
    }

    private void heartbeat(ScheduledExecutorService scheduledExecutorService) {
        timer++;
        if (timer % SECOND_OF_MINUTE == 0) {
            LOGGER.info("It has been continuously capturing packets for {} minutes.", timer / SECOND_OF_MINUTE);
        }
        File dir = new File(config.getFileConfig().getFilePath());
        int count = fileCount + getCurrentFileList(dir).size();
        if (timer / SECOND_OF_MINUTE >= config.getCaptureDuration() || count > config.getFileConfig().getFileCount()) {
            stop(true);
            if (!config.shouldSendFile()) {
                stat();
            }
            scheduledExecutorService.shutdown();
        }
        if (!isNormalStop.get()) {
            while (threadPool.getActiveCount() > 0) {
                sleep(500);
            }
            stat();
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * Build command
     *
     * @return String the transcribe command line
     */
    protected String buildCommand() {
        StringBuilder command = new StringBuilder(config.getPluginPath() + ConfigReader.TCPDUMP);
        for (Map.Entry<String, String> map : parameterMap.entrySet()) {
            command.append(" ").append(map.getKey()).append(" ").append(map.getValue());
        }
        return command.toString();
    }

    /**
     * Initialize transcribe command line parameter
     */
    protected void initCommandParameters() {
        // tcpdump -i lo -s 0 -l -w mysql_tpcc.pcap port 3306 -C 16
        parameterMap.put("-i", config.getNetworkInterface());
        parameterMap.put("-s", "0");
        parameterMap.put("-l", "");
        parameterMap.put("-w", config.getFileConfig().getFilePath() + config.getFileConfig().getFileName()
                + PCAP_SUFFIX);
        parameterMap.put("port", String.valueOf(config.getCapturePort()));
        parameterMap.put("-C", String.valueOf(config.getFileConfig().getFileSize()));
        parameterMap.put("-Z", "root");
    }

    /**
     * Arrange files
     */
    protected void arrangeFiles() {
        File dir = new File(config.getFileConfig().getFilePath());
        List<File> files;
        while (true) {
            files = getCurrentFileList(dir);
            if (isCatchFinished.get() && (!isNormalStop.get() || files.isEmpty())) {
                break;
            }
            sendFiles(files);
            sleep(1000);
        }
        if (isNormalStop.get()) {
            files = getCurrentFileList(dir);
            sendFiles(files);
            LOGGER.info("All result files have been sent, transcribe completed.");
            stat();
        }
        threadPool.shutdown();
    }

    /**
     * stat
     */
    public void stat() {
        File dir = new File(config.getFileConfig().getFilePath());
        int count = fileCount + getCurrentFileList(dir).size();
        LocalDateTime curr = LocalDateTime.now();
        String table = "       ";
        String line = System.lineSeparator();
        String start = TIME_FORMATTER.format(startTime);
        String current = TIME_FORMATTER.format(curr);
        long duration = curr.toEpochSecond(ZoneOffset.UTC) - startTime.toEpochSecond(ZoneOffset.UTC);
        String res = String.format("%s Start Time: %s%s%s End Time: %s%s%s Duration: %s seconds%s%s File Count: %s",
                table, start, line, table, current, line, table, duration, line, table, count);
        LOGGER.info("Transcribe finished, the statistical results are as follows:{}{}", line, res);
    }

    private List<File> getCurrentFileList(File dir) {
        File[] files = dir.listFiles();
        List<File> sendList = new ArrayList<>();
        Map<Integer, File> fileMap = new HashMap<>();
        int maxIndex = -1;
        String name;
        for (File file : files) {
            name = file.getName();
            if (!isValidFile(name)) {
                continue;
            }
            int index = getFileIndex(name);
            fileMap.put(index, file);
            if (index > maxIndex) {
                File validFile = fileMap.get(maxIndex);
                if (validFile != null) {
                    sendList.add(validFile);
                }
                maxIndex = index;
            } else {
                sendList.add(file);
            }
        }
        if (isCatchFinished.get() && maxIndex != -1) {
            sendList.add(fileMap.get(maxIndex));
        }
        return sendList;
    }

    /**
     * Is valid file
     *
     * @param fileName String the file name
     * @return boolean the is valid file
     */
    protected boolean isValidFile(String fileName) {
        return fileName.endsWith(PCAP_SUFFIX) || matchPcapName(fileName);
    }

    /**
     * Get file index
     *
     * @param name String the file name
     * @return int the file index
     */
    protected int getFileIndex(String name) {
        if (name.endsWith(PCAP_SUFFIX)) {
            return 0;
        }
        return Integer.parseInt(name.split(PCAP_SUFFIX)[1]);
    }

    private void sendFiles(List<File> files) {
        if (files.isEmpty()) {
            return;
        }
        String localFilePath = config.getFileConfig().getFilePath();
        String remoteFilePath = config.getRemoteConfig().getRemoteFilePath();
        String username = config.getRemoteConfig().getRemoteReceiverName();
        String password = config.getRemoteConfig().getRemoteReceiverPassword();
        String host = config.getRemoteConfig().getRemoteNodeIp();
        if ("******".equals(password)) {
            scpFiles(localFilePath, remoteFilePath, username, host, files);
            return;
        }
        int port = config.getRemoteConfig().getRemoteNodePort();
        JSch jsch = new JSch();
        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;
        try {
            session = jsch.getSession(username, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp) channel;
            for (File file : files) {
                channelSftp.put(localFilePath + file.getName(), remoteFilePath);
                refreshSendStatus(true, null, file);
            }
            channelSftp.exit();
            retryCount = 0;
        } catch (JSchException | SftpException e) {
            refreshSendStatus(false, e.getMessage(), null);
        } finally {
            if (channelSftp != null) {
                channelSftp.exit();
            }
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    private void scpFiles(String localFilePath, String remoteFilePath, String username, String host, List<File> files) {
        String command;
        Process scpProcess;
        int exitCode;
        try {
            for (File file : files) {
                command = String.format("scp %s %s@%s:%s", localFilePath + file.getName(), username, host,
                        remoteFilePath);
                scpProcess = Runtime.getRuntime().exec(command);
                exitCode = scpProcess.waitFor();
                if (exitCode != 0) {
                    refreshSendStatus(false, "Scp files failed.", null);
                    break;
                }
                refreshSendStatus(true, null, file);
            }
            retryCount = 0;
        } catch (IOException | InterruptedException e) {
            refreshSendStatus(false, e.getMessage(), null);
        }
    }

    private void refreshSendStatus(boolean isSuccess, String errorMsg, File file) {
        if (!isSuccess) {
            retryCount++;
            LOGGER.error("Sending file to {} failed, will retry {} times, already retried {} time...",
                    config.getRemoteConfig().getRemoteNodeIp() + ":" + config.getRemoteConfig().getRemoteFilePath(),
                    config.getRemoteConfig().getRemoteRetryCount(), retryCount);
            if (retryCount >= config.getRemoteConfig().getRemoteRetryCount()) {
                LOGGER.error("Some exceptions occurred while send file, will stop transcribing, error message is: {}.",
                        errorMsg);
                stop(false);
            }
        } else {
            file.delete();
            fileCount++;
            LOGGER.info("The result file {} is sent to {}, receiver is {}.", file.getName(),
                    config.getRemoteConfig().getRemoteNodeIp() + ":" + config.getRemoteConfig().getRemoteFilePath(),
                    config.getRemoteConfig().getRemoteReceiverName());
        }
    }

    private boolean matchPcapName(String name) {
        return name.matches(".*\\.pcap[0-9]+$");
    }

    private void monitorSystemResource(ScheduledExecutorService scheduledExecutorService) {
        monitorDisk();
        monitorMemory();
        monitorCpu();
        if (isCatchFinished.get()) {
            scheduledExecutorService.shutdown();
        }
    }

    private void monitorDisk() {
        double diskUseRate;
        try {
            Process dfProcess = Runtime.getRuntime().exec("df");
            BufferedReader reader = new BufferedReader(new InputStreamReader(dfProcess.getInputStream(),
                    StandardCharsets.UTF_8));
            String line;
            String targetRate = "";
            String root = "";
            String target;
            String[] split;
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("[ \\t]+", " ");
                split = line.split(" ");
                target = split[split.length - 1];
                if (File.separator.equals(target)) {
                    root = split[split.length - 2];
                    continue;
                }
                if (config.getFileConfig().getFilePath().startsWith(target)) {
                    targetRate = split[split.length - 2];
                    break;
                }
            }
            reader.close();
            String res;
            if (!targetRate.isEmpty()) {
                res = targetRate.substring(0, targetRate.length() - 1);
            } else {
                res = root.substring(0, root.length() - 1);
            }
            diskUseRate = Double.parseDouble(res) / 100.0;
            if (diskUseRate >= config.getDiskThreshold()) {
                LOGGER.warn("The current file system Disk usage rate is {}%, which has reached the upper limit {}%."
                        + " Transcribe will be stopped.", res, String.format("%.2f", config.getDiskThreshold() * 100));
                stop(false);
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot check disk usage rate, please manually monitor the disk capacity.");
        }
    }

    private void monitorMemory() {
        double memoryUseRate;
        try {
            Process freeProcess = Runtime.getRuntime().exec("free");
            BufferedReader reader = new BufferedReader(new InputStreamReader(freeProcess.getInputStream(),
                    StandardCharsets.UTF_8));
            String line;
            String[] split;
            double total = 0.0d;
            double free = 0.0d;
            reader.readLine();
            if ((line = reader.readLine()) != null) {
                line = line.replaceAll("[ \\t]+", " ");
                split = line.split(" ");
                total = Double.parseDouble(split[1]);
                free = Double.parseDouble(split[split.length - 1]);
            }
            reader.close();
            memoryUseRate = (total - free) / total;
            if (memoryUseRate >= config.getMemoryThreshold()) {
                LOGGER.warn("The current system Memory usage rate is {}%, which has reached the upper limit {}%."
                                + " Transcribe will be stopped.", String.format("%.2f", memoryUseRate * 100),
                        String.format("%.2f", config.getMemoryThreshold() * 100));
                stop(false);
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot check memory usage rate, please manually monitor the system memory.");
        }
    }

    private void monitorCpu() {
        long[] beforeTicks = getCurrentTicks();
        if (beforeTicks.length == 0) {
            return;
        }
        sleep(1000);
        long[] afterTicks = getCurrentTicks();
        long idleDifference = afterTicks[3] - beforeTicks[3];
        long allDifference = 0L;
        for (int i = 0; i < beforeTicks.length; i++) {
            allDifference += (afterTicks[i] - beforeTicks[i]);
        }
        double cpuUseRate = 1 - (idleDifference / (allDifference * 1.0));
        if (cpuUseRate >= config.getCpuThreshold()) {
            LOGGER.warn("The current system CPU usage rate is {}%, which has reached the upper limit {}%."
                    + " Transcribe will be stopped.", String.format("%.2f", cpuUseRate * 100), String.format("%.2f",
                    config.getCpuThreshold() * 100));
            stop(false);
        }
    }

    private long[] getCurrentTicks() {
        String line;
        try {
            Process statProcess = Runtime.getRuntime().exec("cat /proc/stat");
            BufferedReader reader = new BufferedReader(new InputStreamReader(statProcess.getInputStream(),
                    StandardCharsets.UTF_8));
            line = reader.readLine();
            line = line.replaceAll("[ \\t]+", " ");
            String[] split = line.split(" ");
            long[] res = new long[split.length - 1];
            for (int i = 1; i < split.length; i++) {
                res[i - 1] = Long.parseLong(split[i]);
            }
            reader.close();
            return res;
        } catch (IOException e) {
            LOGGER.warn("Cannot check CPU usage rate, please manually monitor the CPU usage.");
        }
        return new long[0];
    }

    private void stop(boolean isNormalStop) {
        this.isNormalStop.set(isNormalStop);
        if (tcpdump == null) {
            isCatchFinished.set(true);
            return;
        }
        process.stop();
        tcpdump.interrupt();
        isCatchFinished.set(true);
        LOGGER.info("Network traffic collection completed.");
    }
}