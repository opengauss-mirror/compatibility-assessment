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