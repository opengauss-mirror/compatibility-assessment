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

package org.opengauss.tool.replay.task;

import org.opengauss.tool.replay.model.SqlModel;

/**
 * ReplaySubTask
 *
 * @since 2024-07-01
 */
public interface ReplaySubTask {
    /**
     * init
     */
    void init();

    /**
     * distribute sql to thread
     *
     * @param sqlModel sqlModel
     */
    void distribute(SqlModel sqlModel);
}
