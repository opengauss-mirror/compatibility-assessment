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

package org.opengauss.tool.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: Global thread exception handler
 *
 * @author wangzhengyuan
 * @since 2025/02/22
 **/
public class ThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOGGER.error("Uncaught exception occurred in thread {}, error message is: ", t.getName(), e);
        System.exit(-1);
    }
}
