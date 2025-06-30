/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

package org.opengauss.tool.parse.ogparser;

import org.opengauss.tool.parse.ParseThread;
import org.opengauss.tool.parse.object.PacketData;
import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.ProtocolConstant;
import org.opengauss.tool.parse.object.SqlInfo;
import org.opengauss.tool.utils.CommonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

/**
 * Description: openGauss protocol parse
 *
 * @author : wang_zhengyuan
 * @since : 2024/11/04
 */
public class OgMessageParser extends ParseThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(OgMessageParser.class);
    private static final String DEFAULT_STATEMENT_NAME = "00";
    private static final String NULL_HEX = "ffffffff";

    private String database;
    private Map<String, SqlInfo> preparedSqlMap;
    private List<SqlInfo> sqlList;
    private boolean canParse = true;

    /**
     * Constructor
     *
     * @param sessionId String the session id
     */
    public OgMessageParser(String sessionId) {
        super(sessionId);
        preparedSqlMap = new HashMap<>();
        sqlList = new ArrayList<>();
    }

    @Override
    protected boolean isQuitMessage(PacketData packet) {
        byte[] data = packet.getData();
        if (data.length != ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES) {
            return false;
        }
        String requestType = CommonParser.parseByBigEndian(data, 0, 1);
        int len = CommonParser.parseIntByBigEndian(data, 1, ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES);
        if (requestType.equals(ProtocolConstant.OG_HEX_QUIT) && len == ProtocolConstant.OG_DATA_LENGTH_BYTES) {
            quit(packet);
            return true;
        }
        return false;
    }

    @Override
    protected synchronized void setDuration(long microsecondTimestamp) {
        for (SqlInfo sql : sqlList) {
            sql.setExecuteDuration(microsecondTimestamp);
        }
    }

    @Override
    public synchronized void addSqLToQueue() {
        if (incompleteSql != null) {
            sqlQueue.addAll(sqlList);
            incompleteSql = null;
            sqlList.clear();
        }
    }

    @Override
    protected void end() {
        if (incompleteSql != null && incompleteSql.getEndTime() != 0) {
            sqlQueue.addAll(sqlList);
            incompleteSql = null;
            sqlList.clear();
        }
        isParseFinished.set(true);
        LOGGER.debug("Packets from {} have been parsed completed.", sessionId);
        interrupt();
    }

    @Override
    protected void parseRequestPacket(String requestType, PacketData packet) {
        if (database == null) {
            initLoginRequest(packet);
            return;
        }
        List<PacketData> ogPacketList = splitPacket(packet);
        for (PacketData ogPacket : ogPacketList) {
            char type = (char) Integer.parseInt(parsePacketType(ogPacket), 16);
            switch (type) {
                case 'Q':
                    parseSql(ogPacket);
                    break;
                case 'P':
                    parsePreparedSql(ogPacket);
                    break;
                case 'B':
                    parsePreparedParameter(ogPacket);
                    break;
                case 'U':
                    bindBatchParameter(ogPacket);
                    break;
                default:
            }
        }
    }

    private List<PacketData> splitPacket(PacketData packet) {
        List<PacketData> ogPacketList = new ArrayList<>();
        byte[] data = packet.getData();
        int start = 0;
        int length;
        while (start < data.length - ProtocolConstant.OG_DATA_LENGTH_BYTES) {
            length = CommonParser.parseIntByBigEndian(data, start + 1,
                start + ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES);
            PacketData ogPacket = new PacketData();
            ogPacket.copyFrom(packet);
            ogPacket.clonePacketData(data, start, 1 + length);
            ogPacketList.add(ogPacket);
            start += (1 + length);
        }
        return ogPacketList;
    }

    @Override
    protected void initLoginRequest(PacketData packet) {
        if (!canParse) {
            return;
        }
        byte[] data = packet.getData();
        int messageLength = 0;
        try {
            messageLength = CommonParser.parseIntByBigEndian(data, 0, ProtocolConstant.OG_DATA_LENGTH_BYTES);
        } catch (NumberFormatException e) {
            canParse = false;
            return;
        }
        if (messageLength != data.length) {
            canParse = false;
            return;
        }
        Map<String, String> paraMap = new HashMap<>();
        int start = ProtocolConstant.OG_LOGIN_MESSAGE_START_INDEX;
        int end;
        while (start < data.length - 1) {
            end = getStringEndIndex(data, start);
            String key = CommonParser.parseByteToString(data, start, end);
            start = end + 1;
            end = getStringEndIndex(data, start);
            String value = CommonParser.parseByteToString(data, start, end);
            start = end + 1;
            paraMap.put(key, value);
        }
        this.username = paraMap.get("user");
        this.database = paraMap.get("database");
        if (!paraMap.containsKey("search_path")) {
            this.schema = database + ".public";
        } else {
            this.schema = database + "." + paraMap.get("search_path");
        }
    }

    @Override
    protected String parsePacketType(PacketData packet) {
        return CommonParser.parseByBigEndian(packet.getData(), 0, 1);
    }

    @Override
    protected void parseSql(PacketData packet) {
        byte[] data = packet.getData();
        int length = CommonParser.parseIntByBigEndian(data, 1, ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES);
        String sql = CommonParser.parseByteToString(data, ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES,
                1 + length)
            .trim();
        SqlInfo sqlObject = new SqlInfo(packet.getPacketId(), false, sql);
        sqlObject.encapsulateSql(username, schema, sessionId, 0);
        sqlObject.setStartTime(packet.getMicrosecondTimestamp());
        incompleteSql = sqlObject;
        previousSql = sqlObject;
        sqlList.add(sqlObject);
    }

    @Override
    protected void parsePreparedSql(PacketData packet) {
        int start = ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES;
        byte[] data = packet.getData();
        int statementEnd = getStringEndIndex(data, start);
        String statement = DEFAULT_STATEMENT_NAME;
        if (statementEnd != start) {
            statement = CommonParser.parseByteToString(data, start, statementEnd).trim();
        }
        start = statementEnd + 1;
        int sqlEnd = getStringEndIndex(data, start);
        String preparedSql = CommonParser.parseByteToString(data, start, sqlEnd).trim();
        start = sqlEnd + 1;
        int paraNum = CommonParser.parseIntByBigEndian(data, start, start + 2);
        SqlInfo pbeSql = new SqlInfo(paraNum, preparedSql);
        pbeSql.encapsulateSql(username, schema, sessionId);
        start += 2;
        int oid;
        while (start < data.length - 1) {
            oid = CommonParser.parseIntByBigEndian(data, start, start + ProtocolConstant.OG_DATA_LENGTH_BYTES);
            pbeSql.getTypeList().add(OgDataTypeConverter.getOgDataType(oid));
            start += ProtocolConstant.OG_DATA_LENGTH_BYTES;
        }
        preparedSqlMap.put(statement, pbeSql);
    }

    @Override
    protected void parsePreparedParameter(PacketData packet) {
        int start = ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES;
        byte[] data = packet.getData();
        int portalEnd = getStringEndIndex(data, start);
        start = portalEnd + 1;
        String statement = DEFAULT_STATEMENT_NAME;
        int statementEnd = getStringEndIndex(data, start);
        if (statementEnd != start) {
            statement = CommonParser.parseByteToString(data, start, statementEnd).trim();
        }
        start = statementEnd + 1;
        SqlInfo preparedSql;
        if (preparedSqlMap.containsKey(statement)) {
            preparedSql = preparedSqlMap.get(statement).clone();
        } else {
            return;
        }
        preparedSql.setSqlId(packet.getPacketId());
        preparedSql.setStartTime(packet.getMicrosecondTimestamp());
        preparedSql.setPacketId(packet.getPacketId());
        if (!preparedSql.isPbe()) {
            incompleteSql = preparedSql;
            previousSql = preparedSql;
            sqlList.add(preparedSql);
            return;
        }
        int paraFormats = CommonParser.parseIntByBigEndian(data, start, start + 2);
        start += 2;
        List<Integer> paraFormatList = new ArrayList<>(paraFormats);
        for (int i = 0; i < paraFormats; i++) {
            paraFormatList.add(CommonParser.parseIntByBigEndian(data, start, start + 2));
            start += 2;
        }
        int paraValueNum = CommonParser.parseIntByBigEndian(data, start, start + 2);
        start += 2;
        for (int i = 0; i < paraValueNum; i++) {
            PreparedValue paraValue = OgDataTypeConverter.getValue(preparedSql.getTypeList().get(i),
                paraFormatList.get(i), data, start);
            start += paraValue.getOffset();
            preparedSql.getParameterList().add(paraValue);
        }
        incompleteSql = preparedSql;
        previousSql = preparedSql;
        sqlList.add(preparedSql);
    }

    private void bindBatchParameter(PacketData packet) {
        int start = ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES;
        byte[] data = packet.getData();
        int recordCount = CommonParser.parseIntByBigEndian(data, start, start
            + ProtocolConstant.OG_DATA_LENGTH_BYTES);
        start += ProtocolConstant.OG_DATA_TYPE_AND_LENGTH_BYTES;
        String statement = DEFAULT_STATEMENT_NAME;
        int statementEnd = getStringEndIndex(data, start);
        if (statementEnd != start) {
            statement = CommonParser.parseByteToString(data, start, statementEnd).trim();
        }
        SqlInfo preparedSql;
        if (preparedSqlMap.containsKey(statement)) {
            preparedSql = preparedSqlMap.get(statement).clone();
        } else {
            return;
        }
        preparedSql.setSqlId(packet.getPacketId());
        preparedSql.setStartTime(packet.getMicrosecondTimestamp());
        preparedSql.setPacketId(packet.getPacketId());
        if (!preparedSql.isPbe()) {
            incompleteSql = preparedSql;
            previousSql = preparedSql;
            sqlList.add(preparedSql);
            return;
        }
        start = statementEnd + 1;
        start = getParaValueStartIndex(start, data);
        if (start == -1) {
            return;
        }
        int paraNum = preparedSql.getParaNum();
        int paraIndex = 0;
        List<PreparedValue> paraList = preparedSql.getParameterList();
        while (paraList.size() < recordCount * paraNum) {
            PreparedValue paraValue = OgDataTypeConverter.getValue(preparedSql.getTypeList().get(paraIndex % paraNum),
                0, data, start);
            start += paraValue.getOffset();
            preparedSql.getParameterList().add(paraValue);
            paraIndex++;
        }
        incompleteSql = preparedSql;
        previousSql = preparedSql;
        sqlList.add(preparedSql);
    }

    private int getParaValueStartIndex(int start, byte[] data) {
        byte flag1 = data[start];
        byte flag2 = data[start + 1];
        for (int i = start + 2; i < data.length - 1; i++) {
            if (flag1 == data[i] && flag2 == data[i + 1]) {
                return i + 2;
            }
        }
        return -1;
    }

    @Override
    protected int handleField(byte[] data) {
        int point = 0;
        if ((char) data[point] == '1') {
            point = point + 5;
        }
        if ((char) data[point] == '2') {
            point = point + 5;
        }
        if ((char) data[point] == 'T') {
            point++;
            int rowDescLen = hexToDecimal(Arrays.copyOfRange(data, point, point + 4));
            point = point + rowDescLen;
        }
        return point;
    }

    @Override
    protected long parseRowPacket(byte[] data, int dataPoint, List<List<String>> dataList, PacketData packet) {
        long rowCount = 0L;
        int point = dataPoint;
        try {
            while ((char) data[point] == 'D') {
                point = point + 5;
                int fieldCount = hexToDecimal(Arrays.copyOfRange(data, point, point + 2));
                point = point + 2;
                List<String> row = new ArrayList<>();
                for (int i = 0; i < fieldCount; i++) {
                    int columnLen = hexToDecimal(Arrays.copyOfRange(data, point, point + 4));
                    point = point + 4;
                    if (columnLen == -1) {
                        row.add(null);
                        continue;
                    }
                    String text = new String(data, point, columnLen, CHARSET);
                    row.add(text);
                    point = point + columnLen;
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

    @Override
    protected int hexToDecimal(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
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
        if (hexString.toString().equals(NULL_HEX)) {
            return -1;
        }
        return Integer.parseInt(hexString.toString(), 16);
    }
}
