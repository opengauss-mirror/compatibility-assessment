/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.service;

import org.kit.collect.utils.response.RespBean;

import javax.servlet.http.HttpServletResponse;

/**
 * SqlOperation
 *
 * @author liu
 * @since 2023-09-17
 */
public interface SqlOperation {
    /**
     * downloadLinux
     *
     * @param fileType fileType
     * @return RespBean
     */
    RespBean downloadLinux(String fileType);

    /**
     * downloadChrome
     *
     * @param fileType fileType
     * @param response response
     */
    void downloadChrome(String fileType, HttpServletResponse response);
}
