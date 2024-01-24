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
 * scanSingleSql.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class ScanSingleSql {
    private String sql;
    private String id;
    private int line;
    private String tag;
    private String filename;

    /**
     * Construct
     *
     * @param sql : sql.
     * @param id : origin position.
     * @param line : sql line.
     * @param tag : xml tag.
     * @param filename : origin input filename.
     */
    public ScanSingleSql(String sql, String id, int line, String tag, String filename) {
        this.sql = sql;
        this.id = id;
        this.line = line;
        this.tag = tag;
        this.filename = filename;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTag() {
        return tag;
    }

    public String getFilename() {
        return filename;
    }
}
