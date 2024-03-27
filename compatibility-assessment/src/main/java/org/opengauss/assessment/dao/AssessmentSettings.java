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

package org.opengauss.assessment.dao;

/**
 * Assessment settings.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class AssessmentSettings {
    private String dbname;
    private boolean isNeedCreateDatabase = false;
    private boolean isPlugin = false;
    private boolean isExtension = false;
    private int database = -1;
    private String inputDir = null;
    private String outputFile = null;


    /**
     * Construct
     */
    public AssessmentSettings() {
        super();
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public boolean isNeedCreateDatabase() {
        return isNeedCreateDatabase;
    }

    public void setNeedCreateDatabase(boolean isNeedCreateDatabase) {
        this.isNeedCreateDatabase = isNeedCreateDatabase;
    }

    public boolean isPlugin() {
        return isPlugin;
    }

    public void setPlugin(boolean isPlugin) {
        this.isPlugin = isPlugin;
    }

    public boolean isExtension() {
        return isExtension;
    }

    public void setExtension(boolean isExtension) {
        this.isExtension = isExtension;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getInputDir() {
        return inputDir;
    }

    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }
}