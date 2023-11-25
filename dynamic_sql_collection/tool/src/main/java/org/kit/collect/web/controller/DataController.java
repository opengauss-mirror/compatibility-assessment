/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.web.controller;

import org.kit.collect.service.SqlOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;

/**
 * DataController
 *
 * @author liu
 * @since 2023-09-17
 */
@RestController
@RequestMapping("/data")
public class DataController {
    @Autowired
    private SqlOperation operation;

    /**
     * downloadLinux
     *
     * @param response response
     */
    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public void downloadLinux(HttpServletResponse response) {
        operation.download(response);
    }
}