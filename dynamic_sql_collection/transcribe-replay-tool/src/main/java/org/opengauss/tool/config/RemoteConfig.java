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

package org.opengauss.tool.config;

import lombok.Data;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.utils.FileOperator;

import java.util.Properties;

/**
 * Description: Remote node config
 *
 * @author : wang_zhengyuan
 * @since : 2024/06/26
 */
@Data
public class RemoteConfig {
    private String remoteFilePath;
    private String remoteReceiverName;
    private String remoteReceiverPassword;
    private String remoteNodeIp;
    private int remoteNodePort;
    private int remoteRetryCount;

    /**
     * Lode remote node config
     *
     * @param props Properties the props
     */
    public void load(Properties props) {
        this.remoteFilePath = FileOperator.formatFilePath(props.getProperty(ConfigReader.REMOTE_FILE_PATH));
        this.remoteReceiverName = props.getProperty(ConfigReader.REMOTE_RECEIVER_NAME, "root");
        this.remoteReceiverPassword = props.getProperty(ConfigReader.REMOTE_RECEIVER_PASSWORD, "******");
        this.remoteNodeIp = props.getProperty(ConfigReader.REMOTE_NODE_IP, "127.0.0.1");
        this.remoteNodePort = Integer.parseInt(props.getProperty(ConfigReader.REMOTE_NODE_PORT, "22"));
        this.remoteRetryCount = Integer.parseInt(props.getProperty(ConfigReader.REMOTE_RETRY_COUNT, "1"));
    }
}
