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

import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.TimestampObj;
import org.opengauss.tool.utils.CommonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Description: Data type converter
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/15
 */
public final class DataTypeConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTypeConverter.class);
    private static final Map<String, ValueConverter> DATA_TYPE_CONVERTER_MAP = new HashMap<String, ValueConverter>() {
        {
            put("03", DataTypeConverter::convertIntValue);
            put("05", DataTypeConverter::convertDoubleValue);
            put("06", DataTypeConverter::convertNullValue);
            put("0c", DataTypeConverter::convertTimestampValue);
            put("fd", DataTypeConverter::convertStringValue);
            put("fe", DataTypeConverter::convertStringValue);
            put("08", DataTypeConverter::convertLongLongValue);
        }
    };

    /**
     * Get value
     *
     * @param dataType String the data type
     * @param packet   byte[] the packet
     * @param start    int the start index
     * @return PreparedValue the preparedValue
     */
    public static PreparedValue getValue(String dataType, byte[] packet, int start) {
        PreparedValue preparedValue;
        if (DATA_TYPE_CONVERTER_MAP.containsKey(dataType)) {
            preparedValue = DATA_TYPE_CONVERTER_MAP.get(dataType).convert(packet, start);
            preparedValue.setType(dataType);
            return preparedValue;
        }
        return new PreparedValue();
    }

    /**
     * Convert int type
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @return PreparedValue the preparedValue
     */
    public static PreparedValue convertIntValue(byte[] packet, int start) {
        PreparedValue preparedValue = new PreparedValue();
        preparedValue.setValue(String.valueOf(Integer.parseInt(CommonParser.parseByLittleEndian(packet, start,
                start + 4), 16)));
        preparedValue.setOffset(4);
        return preparedValue;
    }

    /**
     * Convert double type
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @return PreparedValue the preparedValue
     */
    public static PreparedValue convertDoubleValue(byte[] packet, int start) {
        long hex = Long.parseLong(CommonParser.parseByLittleEndian(packet, start, start + 8), 16);
        String value = String.valueOf(Double.longBitsToDouble(hex));
        return new PreparedValue(value, 8);
    }

    /**
     * Convert null value
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @return PreparedValue the preparedValue
     */
    public static PreparedValue convertNullValue(byte[] packet, int start) {
        return new PreparedValue("null");
    }

    /**
     * Convert timestamp value
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @return PreparedValue the preparedValue
     */
    public static PreparedValue convertTimestampValue(byte[] packet, int start) {
        int valueLength = Integer.parseInt(CommonParser.parseByLittleEndian(packet, start, start + 1), 16);
        TimestampObj timestamp = new TimestampObj();
        timestamp.setYear(Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 1, start + 3), 16));
        timestamp.setMonth(Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 3, start + 4), 16));
        timestamp.setDay(Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 4, start + 5), 16));
        timestamp.setHour(Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 5, start + 6), 16));
        timestamp.setMinute(Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 6, start + 7), 16));
        int second = Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 7, start + 8), 16);
        int billionthOfSecond = Integer.parseInt(CommonParser.parseByLittleEndian(packet, start + 8, start + 12), 16);
        timestamp.setSecond(second, billionthOfSecond);
        return new PreparedValue(timestamp.toString(), valueLength + 1);
    }

    /**
     * Convert String value
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @return PreparedValue the preparedValue
     */
    public static PreparedValue convertStringValue(byte[] packet, int start) {
        int flag = packet[start] & 0xff;
        int index = start;
        int intLen;
        int offset;
        if (flag == 0xfc) {
            intLen = 2;
            offset = 3;
            index++;
        } else if (flag == 0xfd) {
            intLen = 3;
            offset = 4;
            index++;
        } else {
            intLen = 1;
            offset = 1;
        }
        int valueLength = Integer.parseInt(CommonParser.parseByLittleEndian(packet, index, index + intLen), 16);
        String value = CommonParser.parseByteToString(packet, index + intLen, index + intLen + valueLength);
        return new PreparedValue(value, valueLength + offset);
    }

    private static PreparedValue convertLongLongValue(byte[] packet, int start) {
        PreparedValue preparedValue = new PreparedValue();
        preparedValue.setValue(String.valueOf(Long.parseLong(CommonParser.parseByLittleEndian(packet, start,
            start + 8), 16)));
        preparedValue.setOffset(8);
        return preparedValue;
    }
}
