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

import org.opengauss.tool.parse.ValueConverter;
import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.ProtocolConstant;
import org.opengauss.tool.utils.CommonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Description: openGauss Data type converter
 *
 * @author : wang_zhengyuan
 * @since : 2024/11/09
 */
public final class OgDataTypeConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OgDataTypeConverter.class);
    private static final Map<Integer, String> OID_TO_DATA_TYPE_MAP = new HashMap<>();
    private static final int INT_LENGTH = 4;

    static {
        OID_TO_DATA_TYPE_MAP.put(0, "unspecified");
        OID_TO_DATA_TYPE_MAP.put(21, "int");
        OID_TO_DATA_TYPE_MAP.put(23, "int");
        OID_TO_DATA_TYPE_MAP.put(5545, "int");
        OID_TO_DATA_TYPE_MAP.put(4001, "int");
        OID_TO_DATA_TYPE_MAP.put(4003, "int");
        OID_TO_DATA_TYPE_MAP.put(4005, "int");
        OID_TO_DATA_TYPE_MAP.put(26, "long");
        OID_TO_DATA_TYPE_MAP.put(20, "long");
        OID_TO_DATA_TYPE_MAP.put(4407, "long");
        OID_TO_DATA_TYPE_MAP.put(1700, "numeric");
        OID_TO_DATA_TYPE_MAP.put(1043, "string");
        OID_TO_DATA_TYPE_MAP.put(701, "double");
        OID_TO_DATA_TYPE_MAP.put(1082, "date");
        OID_TO_DATA_TYPE_MAP.put(17, "bytea");
        OID_TO_DATA_TYPE_MAP.put(1560, "bit");
        OID_TO_DATA_TYPE_MAP.put(114, "json");
        OID_TO_DATA_TYPE_MAP.put(88, "blob");
    }

    private static final Map<String, ValueConverter> DATA_TYPE_CONVERTER_MAP = new HashMap<String, ValueConverter>() {
        {
            put("int", OgDataTypeConverter::convertBinaryIntValue);
            put("double", OgDataTypeConverter::convertBinaryDoubleValue);
        }
    };

    private static PreparedValue convertBinaryDoubleValue(byte[] data, int start) {
        int len = CommonParser.parseIntByBigEndian(data, start, start + INT_LENGTH);
        long bits = CommonParser.parseLongByBigEndian(data, start, start + len);
        double value = Double.longBitsToDouble(bits);
        return new PreparedValue("double", String.valueOf(value), INT_LENGTH + len);
    }

    private static PreparedValue convertBinaryIntValue(byte[] data, int start) {
        int len = CommonParser.parseIntByBigEndian(data, start, start + INT_LENGTH);
        if (len <= INT_LENGTH) {
            int value = CommonParser.parseIntByBigEndian(data, start + INT_LENGTH, start + INT_LENGTH + len);
            return new PreparedValue("int", String.valueOf(value), INT_LENGTH + len);
        }
        long value = CommonParser.parseLongByBigEndian(data, start + INT_LENGTH, start + INT_LENGTH + len);
        return new PreparedValue("long", String.valueOf(value), INT_LENGTH + len);
    }

    private static PreparedValue convertStringValue(byte[] data, int start) {
        int nullOffset = 0;
        while (data[start + nullOffset] == -1) {
            nullOffset++;
            if (nullOffset == ProtocolConstant.OG_DATA_LENGTH_BYTES) {
                return new PreparedValue(null, ProtocolConstant.OG_DATA_LENGTH_BYTES);
            }
        }
        int length = CommonParser.parseIntByBigEndian(data, start, start + ProtocolConstant.OG_DATA_LENGTH_BYTES);
        String value = CommonParser.parseByteToString(data, start + ProtocolConstant.OG_DATA_LENGTH_BYTES,
            start + ProtocolConstant.OG_DATA_LENGTH_BYTES + length);
        return new PreparedValue(value, ProtocolConstant.OG_DATA_LENGTH_BYTES + length);
    }

    /**
     * Get openGauss data type according oid
     *
     * @param oid long the data type oid
     * @return String the data type
     */
    public static String getOgDataType(int oid) {
        String dataType = OID_TO_DATA_TYPE_MAP.get(oid);
        if (dataType == null) {
            LOGGER.warn("Unknown OG data type oid: {}, will process by object type", oid);
            return "object";
        }
        return dataType;
    }

    /**
     * Get record value
     *
     * @param dataType String the data type
     * @param data byte[] the data
     * @param startIndex int the start index
     * @return PreparedValue the prepared parameter
     */
    public static PreparedValue getValue(String dataType, int format, byte[] data, int startIndex) {
        PreparedValue preparedValue;
        // 0 is text format, 1 is binary format
        if (format == 1) {
            preparedValue = DATA_TYPE_CONVERTER_MAP.get(dataType).convert(data, startIndex);
        } else {
            preparedValue = convertStringValue(data, startIndex);
            if ("unspecified".equals(dataType)) {
                specificUnknownType(preparedValue);
            } else {
                preparedValue.setType(dataType);
            }
        }

        return preparedValue;
    }

    private static void specificUnknownType(PreparedValue preparedValue) {
        String value = preparedValue.getValue();
        if (value == null) {
            preparedValue.setType("object");
            return;
        }
        if (value.trim().endsWith("+08")) {
            value = value.trim().substring(0, value.trim().length() - 3).trim();
        }
        if (isMatchDate(value)) {
            preparedValue.setType("date");
            preparedValue.setValue(value);
        } else if (isMatchTimestamp(value)) {
            preparedValue.setType("timestamp");
            preparedValue.setValue(value);
        } else if (isMatchTime(value)) {
            preparedValue.setType("time");
            preparedValue.setValue(value);
        } else {
            preparedValue.setType("object");
        }
    }

    private static boolean isMatchTime(String value) {
        try {
            LocalTime localTime = LocalTime.parse(value);
            Time.valueOf(localTime);
        } catch (IllegalArgumentException | DateTimeException e) {
            return false;
        }
        return true;
    }

    private static boolean isMatchTimestamp(String value) {
        try {
            Timestamp.valueOf(value);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    private static boolean isMatchDate(String value) {
        try {
            Date.valueOf(value);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }
}
