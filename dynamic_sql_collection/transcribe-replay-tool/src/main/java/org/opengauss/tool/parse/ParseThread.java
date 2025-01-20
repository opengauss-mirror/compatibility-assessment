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

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opengauss.tool.config.ResultFileConfig;
import org.opengauss.tool.parse.object.PacketData;
import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.ProtocolConstant;
import org.opengauss.tool.parse.object.SqlInfo;
import org.opengauss.tool.parse.object.SelectResult;
import org.opengauss.tool.utils.CommonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description: Parse packet thread
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/05
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ParseThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseThread.class);

    /**
     * Standard charset used for encoding
     */
    protected static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final int FORMAT_FC = 252;

    private static final int FORMAT_FD = 253;

    private static final int FORMAT_FE = 254;

    /**
     * packet queue
     */
    protected BlockingQueue<PacketData> packetQueue;

    /**
     * sql queue
     */
    protected BlockingQueue<SqlInfo> sqlQueue;

    /**
     * source database username
     */
    protected String username;

    /**
     * source database schema
     */
    protected String schema;

    /**
     * incomplete Sql
     */
    protected SqlInfo incompleteSql;

    /**
     * previous Sql
     */
    protected SqlInfo previousSql;

    /**
     * session id
     */
    protected String sessionId;

    /**
     * Configuration for the result file
     */
    protected ResultFileConfig resultFileConfig;

    /**
     * is parse packet finished
     */
    protected AtomicBoolean isParseFinished;
    private Map<Integer, SqlInfo> preparedSqlMap;
    private AtomicBoolean isDistributeFinished;
    private List<Integer> preparedCloseStatements;
    private Set<Long> selectPacketIds = new HashSet<>();

    /**
     * Constructor
     *
     * @param sessionId String the session id
     */
    public ParseThread(String sessionId) {
        this.packetQueue = new LinkedBlockingQueue<>();
        this.sqlQueue = new LinkedBlockingQueue<>();
        this.sessionId = sessionId;
        this.preparedSqlMap = new HashMap<>();
        this.isDistributeFinished = new AtomicBoolean(false);
        this.isParseFinished = new AtomicBoolean(false);
        this.preparedCloseStatements = new ArrayList<>();
        setName("parse " + sessionId);
    }

    public void setFileConfig(ResultFileConfig config) {
        this.resultFileConfig = config;
    }

    /**
     * Add packet data to queue
     *
     * @param packetData PacketData the packet data
     */
    public void addDataToQueue(PacketData packetData) {
        this.packetQueue.add(packetData);
    }

    @Override
    public void run() {
        List<PacketData> packetDataList = new ArrayList<>();
        PacketData mergedPacket;
        while (true) {
            PacketData currentPacket = pollNextPacket();
            if (currentPacket == null) {
                break;
            }
            if (ProtocolConstant.RESPONSE.equals(currentPacket.getPacketType())) {
                if (incompleteSql != null) {
                    setDuration(currentPacket.getMicrosecondTimestamp());
                } else {
                    LOGGER.debug("Parsing SQL from {}.", sessionId);
                }
                if (previousSql == null || !previousSql.isQuery() || !resultFileConfig.isParseResult()) {
                    continue;
                }
            } else {
                addSqLToQueue();
            }
            if (isQuitMessage(currentPacket)) {
                break;
            }
            packetDataList.add(currentPacket);
            PacketData next;
            while (true) {
                next = peekNextPacket();
                if (next == null) {
                    if (ProtocolConstant.REQUEST.equals(currentPacket.getPacketType())) {
                        packetDataList.clear();
                    }
                    LOGGER.debug("The last packet from {} have no it's response packet, ignore it.", sessionId);
                    break;
                }
                if (next.getPacketType().equals(currentPacket.getPacketType())) {
                    PacketData nextPacket = pollNextPacket();
                    packetDataList.add(nextPacket);
                } else {
                    break;
                }
            }
            mergedPacket = mergePacket(packetDataList);
            packetDataList.clear();
            if (ProtocolConstant.RESPONSE.equals(currentPacket.getPacketType())) {
                if (!selectPacketIds.contains(previousSql.getPacketId())) {
                    parseResponsePacket(mergedPacket);
                    selectPacketIds.add(previousSql.getPacketId());
                }
            } else {
                parsePacket(mergedPacket);
            }
        }
        end();
    }

    /**
     * End of the current parse thread
     */
    protected void end() {
        if (incompleteSql != null && incompleteSql.getEndTime() != 0) {
            sqlQueue.add(incompleteSql.clone());
            incompleteSql = null;
        }
        isParseFinished.set(true);
        LOGGER.debug("Packets from {} have been parsed completed.", sessionId);
        interrupt();
    }

    /**
     * Set sql execute duration
     *
     * @param microsecondTimestamp long the microsecond timestamp
     */
    protected void setDuration(long microsecondTimestamp) {
        incompleteSql.setExecuteDuration(microsecondTimestamp);
    }

    /**
     * Is quit message
     *
     * @param packet PacketData the packet data
     * @return true if current massage is quit message
     */
    protected boolean isQuitMessage(PacketData packet) {
        if (packet.getData().length == 5) {
            String requestType = parsePacketType(packet);
            if (ProtocolConstant.COM_QUIT.equals(requestType)) {
                quit(packet);
                return true;
            }
        }
        return false;
    }

    private PacketData mergePacket(List<PacketData> packetDataList) {
        if (packetDataList.isEmpty()) {
            return new PacketData();
        }
        PacketData res = packetDataList.get(0);
        if (packetDataList.size() == 1) {
            return res;
        }
        res.setMicrosecondTimestamp(packetDataList.get(packetDataList.size() - 1).getMicrosecondTimestamp());
        int totalLength = 0;
        for (PacketData packetData : packetDataList) {
            totalLength += packetData.getData().length;
        }
        byte[] mergeData = Arrays.copyOf(packetDataList.get(0).getData(), totalLength);
        int offset = packetDataList.get(0).getData().length;
        for (int i = 1; i < packetDataList.size(); i++) {
            byte[] next = packetDataList.get(i).getData();
            System.arraycopy(next, 0, mergeData, offset, next.length);
            offset += next.length;
        }
        res.setData(mergeData);
        res.setMergeInfo(packetDataList.get(0));
        return res;
    }

    private PacketData pollNextPacket() {
        do {
            PacketData next = packetQueue.poll();
            if (next != null) {
                return next;
            }
        } while (shouldWaitPacket());
        return null;
    }

    private PacketData peekNextPacket() {
        do {
            PacketData next = packetQueue.peek();
            if (next != null) {
                return next;
            }
        } while (shouldWaitPacket());
        return null;
    }

    private boolean shouldWaitPacket() {
        if (isDistributeFinished.get() && packetQueue.isEmpty()) {
            return false;
        }
        try {
            sleep(10);
        } catch (InterruptedException e) {
            LOGGER.error("Sleep interrupted.", e);
        }
        return true;
    }

    /**
     * parse packet
     *
     * @param packet PacketData the packet
     */
    protected void parsePacket(PacketData packet) {
        if (packet.getData() == null) {
            return;
        }
        String requestType = parsePacketType(packet);
        parseRequestPacket(requestType, packet);
    }

    /**
     * parse response packet
     *
     * @param packet PacketData the packet
     */
    protected void parseResponsePacket(PacketData packet) {
        if (packet.getData() == null) {
            return;
        }
        byte[] data = packet.getData();
        int dataPoint = handleField(data);
        List<List<String>> dataList = new ArrayList<>();
        long rowCount = parseRowPacket(data, dataPoint, dataList, packet);
        SelectResult sr = new SelectResult(packet.getPacketId(), previousSql, rowCount, dataList);
        ParseTask.addResultToQueue(sr);
    }

    /**
     * handle field
     *
     * @param data packet data
     * @return data row point
     */
    protected int handleField(byte[] data) {
        int point = 5;
        int fieldNumber = data[4];
        for (int i = 0; i < fieldNumber; i++) {
            int packetLength = hexToDecimal(Arrays.copyOfRange(data, point, point + 3));
            point = point + 4 + packetLength;
        }
        return point;
    }

    /**
     * parse data row
     *
     * @param data packet data
     * @param dataPoint data row point
     * @param dataList data row
     * @param packet packet data
     * @return rows count
     */
    protected long parseRowPacket(byte[] data, int dataPoint, List<List<String>> dataList, PacketData packet) {
        long rowCount = 0L;
        int fieldNumber = data[4];
        int point = dataPoint;
        try {
            while (point < data.length - 11) {
                point = point + 4;
                List<String> row = new ArrayList<>();
                for (int i = 0; i < fieldNumber; i++) {
                    int textLen;
                    int type = data[point] & 0xFF;
                    switch (type) {
                        case FORMAT_FC:
                            textLen = hexToDecimal(Arrays.copyOfRange(data, point + 1, point + 3));
                            point = point + 3;
                            break;
                        case FORMAT_FD:
                            textLen = hexToDecimal(Arrays.copyOfRange(data, point + 1, point + 4));
                            point = point + 4;
                            break;
                        case FORMAT_FE:
                            textLen = hexToDecimal(Arrays.copyOfRange(data, point + 1, point + 9));
                            point = point + 9;
                            break;
                        default:
                            textLen = hexToDecimal(Arrays.copyOfRange(data, point, point + 1));
                            point = point + 1;
                    }
                    String text = new String(data, point, textLen, CHARSET);
                    row.add(text);
                    point = point + textLen;
                }
                rowCount++;
                dataList.add(row);
            }
        } catch (IndexOutOfBoundsException e) {
            LOGGER.error("parse row packet error, fileName is {}, idInFile is {}, packetId is {}, sqlPacketId is {} : ",
                    packet.getLocationFile(), packet.getIdInFile(), packet.getPacketId(), previousSql.getPacketId(), e);
        }
        return rowCount;
    }

    /**
     * convert hex to decimal
     *
     * @param bytes hex bytes
     * @return decimal
     */
    protected int hexToDecimal(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (int i = bytes.length - 1; i >= 0; i--) {
            String hex;
            if (bytes[i] < 0) {
                hex = Integer.toHexString(bytes[i] & 0xFF);
            } else {
                hex = Integer.toHexString(bytes[i]);
            }
            if (hex.length() == 1) {
                hex = "0" + hex;
            }
            hexString.append(hex);
        }
        return Integer.parseInt(hexString.toString(), 16);
    }

    /**
     * Parse request packet
     *
     * @param requestType String the request packet type
     * @param packet PacketData packet
     */
    protected void parseRequestPacket(String requestType, PacketData packet) {
        int payloadLen = CommonParser.parseIntByLittleEndian(packet.getData(), 0, 3);
        if (packet.getData().length < payloadLen + 4) {
            LOGGER.info("Some Packet from {} are lost, ignore them.", sessionId);
            skipResponsePacket();
            return;
        }
        if (packet.getData().length > payloadLen + 4) {
            packet.modifyData(payloadLen + 4);
        }
        int sequenceId = CommonParser.parseIntByLittleEndian(packet.getData(), 3, 4);
        if (!ProtocolConstant.REQUEST_TYPE_LIST.contains(requestType)) {
            try {
                initLoginRequest(packet);
            } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                schema = null;
            }
            return;
        }
        if (schema == null && !ProtocolConstant.COM_QUIT.equals(requestType)) {
            skipResponsePacket();
            return;
        }
        switch (requestType) {
            case ProtocolConstant.COM_QUERY:
                parseSql(packet);
                return;
            case ProtocolConstant.COM_STMT_PREPARE:
                parsePreparedSql(packet);
                return;
            case ProtocolConstant.COM_STMT_EXECUTE:
                parsePreparedParameter(packet);
                return;
            case ProtocolConstant.COM_STMT_CLOSE:
                closeStatement(packet);
                return;
            case ProtocolConstant.COM_STMT_RESET:
                resetStatement(packet);
                return;
            default:
                skipResponsePacket();
        }
    }

    /**
     * Initialize login information
     *
     * @param packet PacketData the packet
     */
    protected void initLoginRequest(PacketData packet) {
        // login request massage，skip 36 bytes, follow is username, end with "00"
        byte[] data = packet.getData();
        int start = 36;
        int end = getStringEndIndex(data, start);
        if (end == -1) {
            return;
        }
        username = CommonParser.parseByteToString(data, start, end);
        int passwordLen = CommonParser.parseIntByLittleEndian(data, end + 1, end + 2);
        start = end + 2 + passwordLen;
        end = getStringEndIndex(data, start);
        schema = CommonParser.parseByteToString(data, start, end);
        PacketData responseData = pollNextPacket();
        if (responseData == null) {
            return;
        }
        skipResponsePacket();
    }

    /**
     * Parse sql
     *
     * @param packet PacketData the packet
     */
    protected void parseSql(PacketData packet) {
        byte[] data = packet.getData();
        String sql = CommonParser.parseByteToString(data, 5, data.length).trim();
        SqlInfo sqlObject = new SqlInfo(packet.getPacketId(), false, sql);
        sqlObject.encapsulateSql(username, schema, sessionId, 0);
        sqlObject.setStartTime(packet.getMicrosecondTimestamp());
        incompleteSql = sqlObject;
        previousSql = sqlObject;
    }

    private PacketData getFinallyResponse() {
        PacketData response = pollNextPacket();
        PacketData next;
        while (true) {
            next = peekNextPacket();
            if (next == null || ProtocolConstant.REQUEST.equals(next.getPacketType())) {
                return response;
            } else {
                response = pollNextPacket();
            }
        }
    }

    /**
     * Parse prepared sql
     *
     * @param packet PacketData the packet
     */
    protected void parsePreparedSql(PacketData packet) {
        byte[] data = packet.getData();
        String sql = CommonParser.parseByteToString(data, 5, data.length).trim();
        SqlInfo sqlObject = new SqlInfo(packet.getPacketId(), true, sql);
        PacketData next = pollNextPacket();
        if (next == null) {
            return;
        }
        String res = parsePacketType(next);
        if (ProtocolConstant.OK_RESPONSE.equals(res)) {
            int statementId = CommonParser.parseIntByLittleEndian(next.getData(), 5, 9);
            int paraNum = CommonParser.parseIntByLittleEndian(next.getData(), 11, 13);
            sqlObject.encapsulateSql(username, schema, sessionId, paraNum);
            preparedSqlMap.put(statementId, sqlObject);
        }
        skipResponsePacket();
    }

    /**
     * Parse prepared parameter
     *
     * @param packet PacketData the packet
     */
    protected void parsePreparedParameter(PacketData packet) {
        byte[] data = packet.getData();
        int statementId = CommonParser.parseIntByLittleEndian(data, 5, 9);
        if (!preparedSqlMap.containsKey(statementId)) {
            skipResponsePacket();
            return;
        }
        SqlInfo pbeSql = preparedSqlMap.get(statementId);
        boolean isNewParas = false;
        int start = 14;
        if (pbeSql.getParaNum() > 0) {
            // NULL bitmap length
            start += (pbeSql.getParaNum() + 7) / 8;
            isNewParas = (CommonParser.parseIntByLittleEndian(data, start, start + 1) == 1);
        }
        List<PreparedValue> parameterList = pbeSql.getParameterList();
        parameterList.clear();
        if (isNewParas) {
            pbeSql.getTypeList().clear();
            for (int i = start + 1; i < start + pbeSql.getParaNum() * 2 + 1; i += 2) {
                // parameter type
                pbeSql.getTypeList().add(CommonParser.parseByLittleEndian(data, i, i + 1));
            }
            start += 1 + pbeSql.getParaNum() * 2;
        } else {
            start += 1;
        }
        int index = 0;
        while (index < pbeSql.getParaNum()) {
            PreparedValue preparedValue = DataTypeConverter.getValue(pbeSql.getTypeList().get(index), data, start);
            if (preparedValue.getValue() == null && preparedValue.getType() == null) {
                LOGGER.error("An error occurred in parsing the {}th message of the file {}, skip it.",
                        packet.getIdInFile(), packet.getLocationFile());
                skipResponsePacket();
                return;
            }
            parameterList.add(preparedValue);
            start += preparedValue.getOffset();
            index++;
        }
        SqlInfo sql = pbeSql.clone();
        sql.setSessionId(sessionId);
        sql.setSqlId(packet.getPacketId());
        sql.setStartTime(packet.getMicrosecondTimestamp());
        sql.setPacketId(packet.getPacketId());
        incompleteSql = sql;
        previousSql = sql;
        pbeSql.getParameterList().clear();
    }

    private void resetStatement(PacketData packet) {
        int statementId = CommonParser.parseIntByLittleEndian(packet.getData(), 5, 9);
        if (preparedSqlMap.containsKey(statementId)) {
            preparedSqlMap.get(statementId).getParameterList().clear();
        }
        skipResponsePacket();
    }

    private void closeStatement(PacketData packet) {
        int statementId = Integer.parseInt(CommonParser.parseByLittleEndian(packet.getData(), 5, 9), 16);
        preparedCloseStatements.add(statementId);
        int len = packet.getData().length;
        // COM_STMT_CLOSE message length
        if (len > 10) {
            packet.clonePacketData(packet.getData(), 9);
            parsePacket(packet);
        }
        skipResponsePacket();
    }

    /**
     * Quit current parse thread
     *
     * @param packet PacketData the packet
     */
    protected void quit(PacketData packet) {
        if (schema == null) {
            return;
        }
        SqlInfo sql = new SqlInfo(packet.getPacketId(), false, "quit");
        sql.encapsulateSql(username, schema, sessionId, 0);
        sqlQueue.add(sql);
    }

    /**
     * Skip response packet
     */
    protected void skipResponsePacket() {
        PacketData next;
        while (true) {
            next = peekNextPacket();
            if (next == null || next.getPacketType().equals(ProtocolConstant.REQUEST)) {
                return;
            }
            pollNextPacket();
        }
    }

    /**
     * Get string end index
     *
     * @param flame byte[] the flame
     * @param start int the start
     *
     * @return int the string end index
     */
    protected int getStringEndIndex(byte[] flame, int start) {
        for (int i = start; i < flame.length; i++) {
            if (flame[i] == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Parse packet type
     *
     * @param packet PacketData the packet
     *
     * @return String the packet type
     */
    protected String parsePacketType(PacketData packet) {
        return CommonParser.parseByLittleEndian(packet.getData(), 4, 5);
    }

    /**
     * Set is distribute finished
     *
     * @param isDistributeFinished boolean the is distribution finished
     */
    public void setIsDistributeFinished(boolean isDistributeFinished) {
        this.isDistributeFinished.set(isDistributeFinished);
    }

    /**
     * Is parse finished
     *
     * @return boolean the isParseFinished
     */
    public boolean isParseFinished() {
        return isParseFinished.get();
    }

    /**
     * Add sql to queue
     */
    public synchronized void addSqLToQueue() {
        if (incompleteSql != null) {
            sqlQueue.add(incompleteSql.clone());
            incompleteSql = null;
        }
    }
}