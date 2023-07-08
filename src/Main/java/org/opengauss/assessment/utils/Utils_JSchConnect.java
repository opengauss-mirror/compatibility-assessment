package org.opengauss.assessment.utils;

import com.jcraft.jsch.*;
import org.opengauss.parser.configure.ConfigureInfo;


public class Utils_JSchConnect {
    private static final String USER = ConfigureInfo.getConfigureInfo().getOsUser();
    private static final String PASSWORD = ConfigureInfo.getConfigureInfo().getOsPassword();
    private static final int DEFAULT_SSH_PORT = 22;

    public static void getJSchConnect(String connectionHost, String connectionPort, String dbname, String pluginName) {
        // 创建JSch对象
        JSch jsch = new JSch();
        Session jschSession = null;
        Channel channel = null;

        // 根据主机账号、ip、端口获取一个Session对象
        try {
            jschSession = jsch.getSession(USER, connectionHost, DEFAULT_SSH_PORT);
            // 存放主机密码
            jschSession.setPassword(PASSWORD);

            // 去掉首次连接确认
            jschSession.setConfig("StrictHostKeyChecking", "no");

            // 进行连接
            jschSession.connect(30000);

            channel = jschSession.openChannel("exec");

            String command = "gsql -d " + dbname + " -p " + connectionPort;

            ((ChannelExec) channel).setCommand(command);

            channel.connect(30000);

            channel.disconnect();

            ((ChannelExec) channel).setCommand("create extension " + pluginName + ";");

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