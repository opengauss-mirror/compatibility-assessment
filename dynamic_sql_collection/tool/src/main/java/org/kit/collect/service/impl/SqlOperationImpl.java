/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.service.impl;

import com.jcraft.jsch.Session;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.kit.collect.config.LinuxConfig;
import org.kit.collect.manager.MonitorManager;
import org.kit.collect.service.SqlOperation;
import org.kit.collect.utils.JschUtil;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;

/**
 * SqlOperationImpl
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
@Service
public class SqlOperationImpl implements SqlOperation {
    @Override
    public void download(HttpServletResponse response) {
        Session session = JschUtil.obtainSession();
        List<String> fileNames = JschUtil.getFileNamesByPath(session, LinuxConfig.getFilePath());
        List<File> files = new ArrayList<>();
        response.setHeader("Content-Disposition", "attachment; filename*=UTF-8''" + "sql_stack.zip");
        try (ZipOutputStream zipOut = new ZipOutputStream(response.getOutputStream())) {
            for (String name : fileNames) {
                Future<File> future = MonitorManager.mine().workExecute(new Callable<File>() {
                    @SneakyThrows
                    @Override
                    public File call() {
                        try (OutputStream out = new FileOutputStream(name)) {
                            JschUtil.downLoad(session, LinuxConfig.getFilePath() + name, out);
                        }
                        return new File(name);
                    }
                });
                files.add(future.get());
            }
            addToZipFile(files, zipOut);
        } catch (IOException | ExecutionException | InterruptedException exception) {
            log.error(exception.getMessage());
        }
        deleteFile(fileNames);
        // close session
        JschUtil.closeSession(session);
    }

    private void addToZipFile(List<File> files, ZipOutputStream zipOut) throws IOException {
        for (File file : files) {
            FileInputStream fileInputStream = new FileInputStream(file);
            ZipEntry zipEntry = new ZipEntry(file.getName());
            zipOut.putNextEntry(zipEntry);
            byte[] bytes = new byte[2048];
            int length;
            while ((length = fileInputStream.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            fileInputStream.close();
        }
    }

    private void deleteFile(List<String> fileNames) {
        fileNames.forEach(item -> {
            File file = new File(item);
            if (file.exists() && file.delete()) {
                log.info("Deleted file: " + item);
            }
        });
    }
}
