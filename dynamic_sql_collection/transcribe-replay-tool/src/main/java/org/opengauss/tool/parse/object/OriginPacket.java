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

import lombok.Data;
import org.opengauss.tool.utils.CommonParser;

/**
 * Description: Origin packet
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/11
 */
@Data
public class OriginPacket {
    private String fileName;
    private int packetIdInFile;
    private byte[] originData;
    private long microsecondTimestamp;
    private String ipType;

    /**
     * Constructor
     *
     * @param fileName             String the packet file name
     * @param packetIdInFile       int the packet id in it's file
     * @param originData           byte[] the origin data
     * @param microsecondTimestamp long the miscrosecond timestamp
     */
    public OriginPacket(String fileName, int packetIdInFile, byte[] originData, long microsecondTimestamp) {
        this.fileName = fileName;
        this.packetIdInFile = packetIdInFile;
        this.originData = originData;
        this.microsecondTimestamp = microsecondTimestamp;
        setIpType(CommonParser.parseByBigEndian(originData, 14, 15));
    }

    private void setIpType(String ipType) {
        if ("60".equals(ipType)) {
            this.ipType = ProtocolConstant.IPV6;
        } else {
            this.ipType = ProtocolConstant.IPV4;
        }
    }
}
