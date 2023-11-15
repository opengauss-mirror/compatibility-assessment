/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;
import org.kit.collect.config.LinuxConfig;
import org.kit.collect.config.StakeConfig;
import org.kit.collect.exception.ParamsException;

/**
 * JschUtil
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class JschUtil {
    /**
     * obtainSession
     *
     * @return Session Session
     */
    public static Session obtainSession() {
        JSch ssh = new JSch();
        Session session = null;
        try {
            session = ssh.getSession(LinuxConfig.getUserName(), LinuxConfig.getHost(), LinuxConfig.getPort());
            session.setPassword(LinuxConfig.getLinuxSecret());
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
        } catch (JSchException exception) {
            log.error("JschUtil obtainSession error-->{}", exception.getMessage());
        }
        return session;
    }

    /**
     * upload
     *
     * @param session session
     */
    public static void upload(Session session) {
        if (session == null) {
            log.error("JschUtil upload occur error session is null,Failed to start uploading the instrumentation file");
            return;
        }
        String uploadPath = LinuxConfig.getUploadPath();
        // Open the sftp channel
        try {
            ChannelSftp channelTrans = new ChannelSftp();
            if (session.openChannel("sftp") instanceof ChannelSftp) {
                channelTrans = (ChannelSftp) session.openChannel("sftp");
            }
            channelTrans.connect();
            // Check if the path exists, create if it does not exist
            try {
                // 判断目录是否存在
                channelTrans.ls(uploadPath);
                log.info("the path already exists, no need to create it");
            } catch (SftpException e) {
                // 目录不存在，创建目录
                channelTrans.mkdir(uploadPath);
                log.info("directory created successfully");
            }
            channelTrans.cd(uploadPath);
            InputStream agentStream = JschUtil.class.getClassLoader().getResourceAsStream(
                    StakeConfig.getResourcePath() + StakeConfig.getAgentName());
            InputStream attachStream = JschUtil.class.getClassLoader().getResourceAsStream(
                    StakeConfig.getResourcePath() + StakeConfig.getAttachName());
            channelTrans.put(agentStream, StakeConfig.getAgentName(), ChannelSftp.OVERWRITE);
            channelTrans.put(attachStream, StakeConfig.getAttachName(), ChannelSftp.OVERWRITE);
            channelTrans.disconnect();
            log.info("successfully uploaded the instrumentation file");
        } catch (JSchException | SftpException exception) {
            log.error("Failed to upload files to Linux, please check the configuration information");
        }
    }

    /**
     * downLoad
     *
     * @param session      session
     * @param linuxPath    linuxPath
     * @param downloadPath downloadPath
     * @param out          out
     */
    public static void downLoad(Session session, String linuxPath, String downloadPath, OutputStream out) {
        try {
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = new ChannelSftp();
            if (channel instanceof ChannelSftp) {
                sftpChannel = (ChannelSftp) channel;
            }
            if (ObjectUtil.isNotEmpty(out)) {
                sftpChannel.get(linuxPath, out);
                sftpChannel.disconnect();
            } else {
                // 下载到linux
                // Check if the destination directory exists, and create it if necessary
                String destinationDirectory = downloadPath.substring(0, downloadPath.lastIndexOf('/'));
                SftpATTRS attrs = null;
                try {
                    attrs = sftpChannel.lstat(destinationDirectory);
                } catch (SftpException e) {
                    log.info("Directory doesn't exist,start create it");
                }
                if (attrs == null || !attrs.isDir()) {
                    sftpChannel.mkdir(destinationDirectory);
                }
                // Download the file
                sftpChannel.get(linuxPath, downloadPath);
                sftpChannel.disconnect();
            }
            log.info("File downloaded successfully.");
        } catch (JSchException | SftpException exception) {
            log.error("File downloaded fail-->{}", exception.getMessage());
            throw new ParamsException("File downloaded fail");
        }
    }

    /**
     * executeCommand
     *
     * @param session session
     * @param command command
     * @return List<String> list
     */
    public static String executeCommand(Session session, String command) {
        if (session == null) {
            log.error("error reported during command execution, session is empty");
            return "";
        }
        StringBuffer res = new StringBuffer();
        try {
            ChannelExec channelExec = new ChannelExec();
            if (session.openChannel("exec") instanceof ChannelExec) {
                channelExec = (ChannelExec) session.openChannel("exec");
            }
            channelExec.setCommand(command);
            channelExec.setInputStream(null);
            channelExec.setErrStream(System.err);
            channelExec.connect();
            InputStream in = channelExec.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
            String line;
            while ((line = reader.readLine()) != null) {
                if (StrUtil.isNotEmpty(line)) {
                    log.info(line);
                    res.append(line);
                }
            }
            reader.close();
            channelExec.disconnect();
        } catch (JSchException | IOException exception) {
            log.error("Failed to execute startup instrumentation command-->{}", exception.getMessage());
        }
        return res.toString();
    }

    /**
     * closeSession
     *
     * @param session session
     */
    public static void closeSession(Session session) {
        if (session != null) {
            session.disconnect();
        }
    }

    /**
     * executeTask
     *
     * @param command command
     */
    public static void executeTask(String command) {
        Session session = JschUtil.obtainSession();
        if (session == null) {
            log.error("connection to linux fails to obtain session");
        } else {
            log.info("Starting to upload instrumentation files");
            JschUtil.upload(session);
            log.info("start executing the pile insertion start command");
            log.info("------------------------------");
            log.info(command);
            log.info("------------------------------");
            executeCommand(session, command);
            log.info("close session");
            closeSession(session);
        }
    }
}
