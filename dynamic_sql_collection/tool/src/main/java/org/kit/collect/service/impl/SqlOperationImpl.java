/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.io.OutputStream;
import org.kit.collect.common.Constant;
import org.kit.collect.config.FileDownloadConfig;
import org.kit.collect.exception.ParamsException;
import org.kit.collect.service.SqlOperation;
import org.kit.collect.utils.AssertUtil;
import org.kit.collect.utils.JschUtil;
import org.kit.collect.utils.response.RespBean;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;

/**
 * SqlOperationImpl
 *
 * @author liu
 * @since 2023-09-17
 */
@Service
public class SqlOperationImpl implements SqlOperation {
    @Override
    public RespBean downloadLinux(String fileType) {
        String downloadPath = getPath(fileType);
        downLoad(fileType, downloadPath, null);
        return RespBean.success("Download successful");
    }

    @Override
    public void downloadChrome(String fileType, HttpServletResponse response) {
        String fileFullName = "";
        String downloadPath = getPath(fileType);
        if (fileType.equals(Constant.SQL_TYPE)) {
            fileFullName = Constant.SQL_NAME;
        } else {
            fileFullName = Constant.STACK_NAME;
        }
        try {
            response.setHeader("Content-Disposition", "attachment; filename*=UTF-8''" + fileFullName);
            OutputStream out = response.getOutputStream();
            downLoad(fileType, downloadPath, out);
        } catch (IOException exception) {
            throw new ParamsException("The download failed because of an abnormal IO stream.");
        }
    }

    private void downLoad(String fileType, String downloadPath, OutputStream out) {
        Session session = JschUtil.obtainSession();
        AssertUtil.isTrue(ObjectUtil.isEmpty(session), "Download failed, get session failed");
        String path = null;
        if (fileType.equals(Constant.SQL_TYPE)) {
            path = Constant.SQL_PATH;
        }
        if (fileType.equals(Constant.STACK_TYPE)) {
            path = Constant.STACK_PATH;
        }
        JschUtil.downLoad(session, path, downloadPath, out);
    }

    private String getPath(String fileType) {
        if (fileType.equals(Constant.SQL_TYPE)) {
            return FileDownloadConfig.getDownloadPathSql();
        } else {
            return FileDownloadConfig.getDownloadPathStack();
        }
    }
}
