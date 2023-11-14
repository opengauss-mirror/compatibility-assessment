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

package org.opengauss.parser;

import org.opengauss.parser.configure.AssessmentInfoManager;
import org.opengauss.parser.filehandler.FileHandler;
import org.opengauss.parser.filehandler.SingleFileHandler;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;
import java.util.Map;

/**
 * Description: Distribute files by file types
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class FileDistributer {
    private FileHandler handler;

    /**
     * Constructor
     */
    public FileDistributer() {
        handler = new SingleFileHandler();
    }

    /**
     * Distribute files in dataDir
     *
     * @param dataDir String
     * @return List<List < File>>
     */
    public Map<String, List<File>> distributeFiles(String dataDir) {
        AtomicReference<Integer> totalfiles = new AtomicReference<>(0);
        File dir = new File(dataDir);
        listAllFiles(dir);
        handler.getFileList().forEach((extension, list) -> {
            totalfiles.updateAndGet(v -> v + list.size());
        });
        AssessmentInfoManager.getInstance().setOutputSqlFileCount(totalfiles.get());
        AssessmentInfoManager.getInstance().setAssessmentFlag(true);
        return handler.getFileList();
    }

    private void listAllFiles(File dir) {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                listAllFiles(file);
            } else {
                handler.handleFile(file);
            }
        }
    }
}
