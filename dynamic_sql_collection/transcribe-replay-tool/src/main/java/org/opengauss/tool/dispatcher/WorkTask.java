/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
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

package org.opengauss.tool.dispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: Work task
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/25
 */
public abstract class WorkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkTask.class);

    /**
     * Start task
     */
    public abstract void start();

    /**
     * Stat
     */
    public abstract void stat();

    /**
     * sleep
     *
     * @param millis long the sleep duration
     */
    public void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException occurred while arranging packet files, error message is: {}.",
                    e.getMessage());
        }
    }
}
