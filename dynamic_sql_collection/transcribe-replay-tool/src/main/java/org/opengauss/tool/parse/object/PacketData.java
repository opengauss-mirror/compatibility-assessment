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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Description: Packet data
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/05
 */
@Data
public class PacketData {
    private String locationFile;
    private int idInFile;
    private long packetId;
    private String packetType;
    private byte[] data;
    private String sourceIp;
    private int sourcePort;
    private String destinationIp;
    private int destinationPort;
    private String clientId;
    private AtomicLong microsecondTimestamp;
    private long seqNum;

    /**
     * Constructor
     */
    public PacketData() {
        this.microsecondTimestamp = new AtomicLong();
    }

    /**
     * Constructor
     *
     * @param packetId   long the packet id
     * @param packetType String the packet type
     */
    public PacketData(long packetId, String packetType) {
        this.packetId = packetId;
        this.packetType = packetType;
        this.microsecondTimestamp = new AtomicLong();
    }

    /**
     * Build packet data
     *
     * @param sourceIp        String the source ip
     * @param sourcePort      int the source port
     * @param destinationIp   String the destination ip
     * @param destinationPort int the destination port
     */
    public void build(String sourceIp, int sourcePort, String destinationIp, int destinationPort) {
        this.sourceIp = sourceIp;
        this.sourcePort = sourcePort;
        this.destinationIp = destinationIp;
        this.destinationPort = destinationPort;
        if (ProtocolConstant.RESPONSE.equals(packetType)) {
            this.clientId = destinationIp + ":" + destinationPort;
        } else {
            this.clientId = sourceIp + ":" + sourcePort;
        }
    }

    /**
     * Clone packet data
     *
     * @param packet byte[] the packet data
     * @param start  int the start index of packet data
     */
    public void clonePacketData(byte[] packet, int start) {
        this.data = Arrays.copyOfRange(packet, start, packet.length);
    }

    public void clonePacketData(byte[] data, int start, int length) {
        this.data = Arrays.copyOfRange(data, start, start + length);
    }

    /**
     * Set microsecond timestamp
     *
     * @param microsecondTimestamp long the microsecond timestamp
     */
    public void setMicrosecondTimestamp(long microsecondTimestamp) {
        this.microsecondTimestamp.set(microsecondTimestamp);
    }

    /**
     * Get microsecond timestamp
     *
     * @return microsecondTimestamp long the microsecond timestamp
     */
    public long getMicrosecondTimestamp() {
        return microsecondTimestamp.get();
    }

    /**
     * Set origin packet information
     *
     * @param originPacket OriginPacket the originPacket
     */
    public void setOriginInfo(OriginPacket originPacket) {
        this.locationFile = originPacket.getFileName();
        this.idInFile = originPacket.getPacketIdInFile();
        this.microsecondTimestamp.set(originPacket.getMicrosecondTimestamp());
    }

    /**
     * Set merge information
     *
     * @param packetData PacketData the packetData
     */
    public void setMergeInfo(PacketData packetData) {
        this.packetId = packetData.getPacketId();
        this.locationFile = packetData.getLocationFile();
        this.idInFile = packetData.getIdInFile();
    }

    /**
     * Modify MySQL message data
     *
     * @param length int the valid data length
     */
    public void modifyData(int length) {
        byte[] newData = new byte[length];
        System.arraycopy(data, 0, newData, 0, length);
        this.data = newData;
    }

    /**
     * Copy attribute from other object
     *
     * @param packet Other PacketData object
     */
    public void copyFrom(PacketData packet) {
        this.locationFile = packet.locationFile;
        this.idInFile = packet.idInFile;
        this.packetId = packet.packetId;
        this.packetType = packet.packetType;
        this.sourceIp = packet.sourceIp;
        this.sourcePort = packet.sourcePort;
        this.destinationIp = packet.destinationIp;
        this.destinationPort = packet.destinationPort;
        this.clientId = packet.clientId;
        this.microsecondTimestamp.set(packet.microsecondTimestamp.get());
    }
}
