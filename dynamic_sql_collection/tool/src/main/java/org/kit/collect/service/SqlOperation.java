/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.service;

import javax.servlet.http.HttpServletResponse;

/**
 * SqlOperation
 *
 * @author liu
 * @since 2023-09-17
 */
public interface SqlOperation {
    /**
     * download
     *
     * @param response response
     */
    void download(HttpServletResponse response);
}
