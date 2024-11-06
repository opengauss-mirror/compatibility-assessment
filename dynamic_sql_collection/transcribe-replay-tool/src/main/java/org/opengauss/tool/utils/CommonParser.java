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

package org.opengauss.tool.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Description: Common protocol parser
 *
 * @author wangzhengyuan
 * @since 2024/06/28
 **/
public final class CommonParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonParser.class);
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private CommonParser() {
    }

    /**
     * Parse int accord little endian
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @param end    int the end index
     * @return String the hexadecimal number
     */
    public static String parseByLittleEndian(byte[] packet, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = end - 1; i >= start; i--) {
            sb.append(String.format("%02x", packet[i] & 0xFF));
        }
        return sb.toString();
    }

    /**
     * Parse int accord big endian
     *
     * @param packet byte[] the packet
     * @param start  int the start index
     * @param end    int the end index
     * @return String the hexadecimal number
     */
    public static String parseByBigEndian(byte[] packet, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(String.format("%02x", packet[i] & 0xFF));
        }
        return sb.toString();
    }

    /**
     * Parse byte to String
     *
     * @param flame byte[] the data flame
     * @param start int the start index
     * @param end   int the end index
     * @return String the parse result
     */
    public static String parseByteToString(byte[] flame, int start, int end) {
        return new String(flame, start, end - start, CHARSET);
    }

    /**
     * Parse timestamp
     *
     * @param packet byte[] the packet
     * @return long the parse result
     */
    public static long parseTimestamp(byte[] packet) {
        int high = Integer.parseInt(CommonParser.parseByLittleEndian(packet, 0, 4), 16);
        int low = Integer.parseInt(CommonParser.parseByLittleEndian(packet, 4, 8), 16);
        return (long) high * 1000 * 1000 + low;
    }

    /**
     * Parse int by big endian
     *
     * @param packet byte[] the packet
     * @param start int the start index
     * @param end int the end index
     *
     * @return int the parse result
     */
    public static int parseIntByBigEndian(byte[] packet, int start, int end) {
        return Integer.parseInt(parseByBigEndian(packet, start, end), 16);
    }

    /**
     * Parse int by big endian
     *
     * @param packet byte[] the packet
     * @param start int the start index
     * @param end int the end index
     *
     * @return int the parse result
     */
    public static int parseIntByLittleEndian(byte[] packet, int start, int end) {
        return Integer.parseInt(parseByLittleEndian(packet, start, end), 16);
    }
}
