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
 * psql settings.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class PsqlSettings {
    private String proname;
    private String dbname;

    /**
     * Construct
     */
    public PsqlSettings() {
        super();
    }

    /**
     * Construct
     *
     * @param proname : process name
     * @param dbname  : dbname
     */
    public PsqlSettings(String proname, String dbname) {
        this.proname = proname;
        this.dbname = dbname;
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String getProname() {
        return proname;
    }

    public void setProname(String proname) {
        this.proname = proname;
    }
}
