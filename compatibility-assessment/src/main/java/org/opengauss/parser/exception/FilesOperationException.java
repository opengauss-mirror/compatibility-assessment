/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

package org.opengauss.parser.exception;

/**
 * Description: files operation exception
 *
 * @author jianghongbo
 * @since 2024/8/30
 */
public class FilesOperationException extends RuntimeException {
    /**
     * Constructor
     */
    public FilesOperationException() {
        super();
    }

    /**
     * Constructor
     *
     * @param message String
     */
    public FilesOperationException(String message) {
        super(message);
    }
}
