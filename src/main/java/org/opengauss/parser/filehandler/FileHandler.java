/*
 * Copyright (c) 2023-2023 Huawei Technologies Co.,Ltd.
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

package org.opengauss.parser.filehandler;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Description: handle File interface.
 *
 * @author jianghongbo
 * @since 2023/7/14
 */
public interface FileHandler {
    /**
     * handle single file
     *
     * @param file File
     */
    void handleFile(File file);

    /**
     * return file list by key
     *
     * @return Map<String,List<File>>
     */
    Map<String, List<File>> getFileList();
}
