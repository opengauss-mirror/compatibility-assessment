/*
 * Copyright (c) 2023-2023 Huawei Technologies Co.,Ltd.
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

package org.opengauss.assessment.utils;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.JSch;
import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;

/**
 * Jsch connection utils.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class JSchConnectionUtils {
    private static final String USER = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OSUSER);
    private static final String
            PASSWORD = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OSPASSWORD);
    private static final int DEFAULT_SSH_PORT = 22;

    /**
     * Get jsch connection and create whale/dolphin plugin.
     *
     * @param connectionHost : remote host
     * @param connectionPort : remote host port.
     * @param dbname : assessment database name.
     * @param pluginName : plugin name.
     */
    public static void getJSchConnect(String connectionHost, String connectionPort, String dbname, String pluginName) {
        // Create JSch object
        JSch jsch = new JSch();
        Session jschSession = null;
        Channel channel = null;

        // Obtain a Session object based on the host account, IP, and port
        try {
            jschSession = jsch.getSession(USER, connectionHost, DEFAULT_SSH_PORT);
            // Store host password
            jschSession.setPassword(PASSWORD);
            // Remove first connection confirmation
            jschSession.setConfig("StrictHostKeyChecking", "no");
            // Get connection
            jschSession.connect(30000);
            channel = jschSession.openChannel("exec");
            String command = "gsql -d " + dbname + " -p " + connectionPort;
            if (channel instanceof ChannelExec) {
                ((ChannelExec) channel).setCommand(command);
            }
            channel.connect(30000);
            channel.disconnect();
            if (channel instanceof ChannelExec) {
                ((ChannelExec) channel).setCommand("create extension " + pluginName + ";");
            }
            channel.connect();
        } catch (JSchException e) {
            e.printStackTrace();
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
                if (jschSession != null && jschSession.isConnected()) {
                    jschSession.disconnect();
                }
            }
        }
    }
}