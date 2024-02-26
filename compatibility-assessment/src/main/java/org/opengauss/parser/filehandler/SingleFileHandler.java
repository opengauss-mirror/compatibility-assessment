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

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Description: put each file into map according to extension.
 *
 * @author Administrator
 * @since 2023/7/14
 */
public class SingleFileHandler implements FileHandler {
    /**
     * sql file extension
     */
    public static final String SQLFILE_EXTENSION = "sql";

    /**
     * general log file extension
     */
    public static final String GENERALLOG_EXTENSION = "general";

    /**
     * slow log file extension
     */
    public static final String SLOWLOG_EXTENSION = "slow";

    /**
     * mybatis/ibatis mapper file extension
     */
    public static final String MAPPER_EXTENSION = "xml";

    /**
     * dynamic insertion output file extension
     */
    public static final String ATTACH_EXTENSION = "attach";
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileHandler.class);
    private static Set<String> fileExtensions = new HashSet<>
            (Arrays.asList(SQLFILE_EXTENSION, GENERALLOG_EXTENSION, SLOWLOG_EXTENSION, MAPPER_EXTENSION,
                    ATTACH_EXTENSION));

    private Map<String, List<File>> extensionTofiles = new HashMap<>();

    /**
     * handle single file.
     *
     * @param file File
     */
    public void handleFile(File file) {
        String extension;
        try {
            extension = FilenameUtils.getExtension(file.getCanonicalPath());
            if (!fileExtensions.contains(extension)) {
                return;
            }
            if (!extensionTofiles.containsKey(extension)) {
                extensionTofiles.put(extension, new ArrayList<>());
            }
            extensionTofiles.get(extension).add(file);
        } catch (IOException exp) {
            LOGGER.error("handle file %s occur IOException.", file.getAbsolutePath());
        }
    }

    /**
     * return extensionTofiles.
     *
     * @return Map<String, List < File>>
     */
    public Map<String, List<File>> getFileList() {
        return extensionTofiles;
    }
}
