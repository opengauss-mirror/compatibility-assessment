/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * transcribe-replay-tool is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.parse;

import com.alibaba.fastjson2.JSONObject;
import org.opengauss.tool.config.parse.ParseConfig;
import org.opengauss.tool.parse.object.PacketData;
import org.opengauss.tool.utils.FileOperator;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Description: pre-parse packet file
 *
 * @author : wangzhengyuan1
 * @since : 2026/08/31
 */
public class PreparseTask extends ParseTask {
    /**
     * Constructor
     *
     * @param config ParseConfig the config
     */
    public PreparseTask(ParseConfig config) {
        super(config);
    }

    @Override
    public void start() {
        threadPool.execute(this::readPcapFile);
        threadPool.execute(this::distributeData);
    }

    @Override
    protected void initStorage() {
    }

    @Override
    protected void distribute(PacketData packetData) {
        String clientId = packetData.getClientId();
        sessionEndPacketMapping.put(clientId, packetData.getPacketId());
    }

    @Override
    protected void stop() {
        List<String> endSessionPacketList = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iterator = sessionEndPacketMapping.entrySet().iterator();
        JSONObject json = new JSONObject();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            json.clear();
            json.fluentPut("session", entry.getKey())
                    .fluentPut("endPacketId", entry.getValue());
            endSessionPacketList.add(json.toJSONString());
            iterator.remove();
        }
        String targetPath = config.getPacketFilePath().endsWith(File.separator) ? config.getPacketFilePath()
                : config.getPacketFilePath() + File.separator;
        FileOperator.removeIfExist(targetPath + "session-end-packet-mapping.json");
        FileOperator.writeStringToFile(endSessionPacketList, targetPath + "session-end-packet-mapping.json");
        threadPool.shutdown();
    }
}
