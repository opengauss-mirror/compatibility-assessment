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
import org.opengauss.tool.parse.object.PacketData;
import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.ProtocolConstant;
import org.opengauss.tool.parse.object.SqlInfo;
import org.opengauss.tool.utils.CommonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private SqlInfo incompleteSql;
    private Map<Integer, SqlInfo> preparedSqlMap;
    private String sessionId;
    private AtomicBoolean isDistributeFinished;
    private AtomicBoolean isParseFinished;
    private List<Integer> preparedCloseStatements;

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

    /**
     * Add packet data to queue
     *
     * @param packetData PacketData the packet data
     */
    public void addDataToQueue(PacketData packetData) {
        this.packetQueue.offer(packetData);
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
                    incompleteSql.setExecuteDuration(currentPacket.getMicrosecondTimestamp());
                } else {
                    LOGGER.debug("Parsing SQL from {}.", sessionId);
                }
                continue;
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
            parsePacket(mergedPacket);
            packetDataList.clear();
        }
        if (incompleteSql != null && incompleteSql.getEndTime() != 0) {
            sqlQueue.offer(incompleteSql.clone());
            incompleteSql = null;
        }
        isParseFinished.set(true);
        LOGGER.debug("Packets from {} have been parsed completed.", sessionId);
        interrupt();
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

    private void parsePacket(PacketData packet) {
        if (packet.getData() == null) {
            return;
        }
        String requestType = parsePacketType(packet);
        parseRequestPacket(requestType, packet);
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
            initLoginRequest(packet);
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
        // "00" means login success
        String res = parsePacketType(responseData);
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

    private void parsePreparedSql(PacketData packet) {
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

    private void parsePreparedParameter(PacketData packet) {
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
        incompleteSql = sql;
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

    private void quit(PacketData packet) {
        if (schema == null) {
            return;
        }
        SqlInfo sql = new SqlInfo(packet.getPacketId(), false, "quit");
        sql.encapsulateSql(username, schema, sessionId, 0);
        sqlQueue.offer(sql);
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

    private String parsePacketType(PacketData packet) {
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
    public void addSqLToQueue() {
        if (incompleteSql != null) {
            sqlQueue.offer(incompleteSql.clone());
            incompleteSql = null;
        }
    }
}