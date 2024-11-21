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

import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.ProtocolConstant;
import org.opengauss.tool.utils.CommonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static {
        OID_TO_DATA_TYPE_MAP.put(0, "object");
        OID_TO_DATA_TYPE_MAP.put(21, "int");
        OID_TO_DATA_TYPE_MAP.put(23, "int");
        OID_TO_DATA_TYPE_MAP.put(5545, "int");
        OID_TO_DATA_TYPE_MAP.put(4001, "int");
        OID_TO_DATA_TYPE_MAP.put(4003, "int");
        OID_TO_DATA_TYPE_MAP.put(4005, "int");
        OID_TO_DATA_TYPE_MAP.put(26, "long");
        OID_TO_DATA_TYPE_MAP.put(20, "long");
        OID_TO_DATA_TYPE_MAP.put(4407, "long");
        OID_TO_DATA_TYPE_MAP.put(1043, "string");
        OID_TO_DATA_TYPE_MAP.put(701, "double");
        OID_TO_DATA_TYPE_MAP.put(1082, "date");
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
    public static PreparedValue getValue(String dataType, byte[] data, int startIndex) {
        PreparedValue preparedValue = convertStringValue(data, startIndex);
        preparedValue.setType(dataType);
        return preparedValue;
    }
}
