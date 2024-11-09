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

package org.opengauss.tool.parse.object;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: Protocol constant value
 *
 * @author : wang_zhengyuan
 * @since : 2024/07/05
 */
public final class ProtocolConstant {
    /**
     * Request type list of MySQL packet
     */
    public static final List<String> REQUEST_TYPE_LIST;

    /**
     * Response type list of MySQL packet
     */
    public static final List<String> RESPONSE_TYPE_LIST;

    /**
     * ipv4
     */
    public static final String IPV4 = "IPV4";

    /**
     * ipv6
     */
    public static final String IPV6 = "IPV6";

    /**
     * response packet type
     */
    public static final String RESPONSE = "response";

    /**
     * request packet type
     */
    public static final String REQUEST = "request";

    /**
     * query packet type
     */
    public static final String COM_QUERY = "03";

    /**
     * quit packet type
     */
    public static final String COM_QUIT = "01";

    /**
     * statement prepared packet type
     */
    public static final String COM_STMT_PREPARE = "16";

    /**
     * statement execute packet type
     */
    public static final String COM_STMT_EXECUTE = "17";

    /**
     * statement close packet type
     */
    public static final String COM_STMT_CLOSE = "19";

    /**
     * statement reset packet type
     */
    public static final String COM_STMT_RESET = "1a";

    /**
     * ok response packet
     */
    public static final String OK_RESPONSE = "00";

    /**
     * packet header length
     */
    public static final int PCAP_HEADER_LENGTH = 24;

    /**
     * packet header length
     */
    public static final int PACKET_HEADER_LENGTH = 16;

    /**
     * Ethernet flame header length
     */
    public static final int ETHERNET_HEADER_LENGTH = 14;

    /**
     * IPV4 header length
     */
    public static final int IPV4_HEADER_LENGTH = 20;

    /**
     * ipv6 header length
     */
    public static final int IPV6_HEADER_LENGTH = 40;

    /**
     * Record the number of bytes in the message length of openGauss protocol
     */
    public static final int OG_DATA_LENGTH_BYTES = 4;

    /**
     * Record the number of bytes in the message type and message length of openGauss protocol
     */
    public static final int OG_DATA_TYPE_AND_LENGTH_BYTES = 5;

    /**
     * Login message start byte index of openGauss protocol
     */
    public static final int OG_LOGIN_MESSAGE_START_INDEX = 8;

    /**
     * Quit message hex string of openGauss protocol of openGauss protocol
     */
    public static final String OG_HEX_QUIT = "58";

    static {
        REQUEST_TYPE_LIST = new ArrayList<>();
        char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd'};
        for (char c : hexChars) {
            REQUEST_TYPE_LIST.add("0" + c);
            REQUEST_TYPE_LIST.add("1" + c);
        }
        RESPONSE_TYPE_LIST = new ArrayList<>();
        REQUEST_TYPE_LIST.add("00");
        REQUEST_TYPE_LIST.add("0a");
        RESPONSE_TYPE_LIST.add("ff");
    }
}
