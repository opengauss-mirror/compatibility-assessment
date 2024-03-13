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
 * sql compatibility.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class SQLCompatibility {
    private String id = "";
    private int line;
    private String sql;
    private AssessmentType sqlType;
    private CompatibilityType compatibilityType;
    private String errDetail;
    private String file;

    /**
     * Construct
     *
     * @param id                : origin position.
     * @param line              : sql line.
     * @param sql               : sql.
     * @param sqlType           : sql type.
     * @param compatibilityType : assessment result type.
     * @param errDetail         : error information.
     * @param file              : original filename information.
     */
    public SQLCompatibility(String id, int line, String sql, AssessmentType sqlType,
                            CompatibilityType compatibilityType, String errDetail, String file) {
        this.id = id;
        this.line = line;
        this.sql = sql;
        this.sqlType = sqlType;
        this.compatibilityType = compatibilityType;
        this.errDetail = errDetail;
        this.file = file;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public AssessmentType getSqlType() {
        return sqlType;
    }

    public void setSqlType(AssessmentType sqlType) {
        this.sqlType = sqlType;
    }

    public CompatibilityType getCompatibilityType() {
        return compatibilityType;
    }

    public void setCompatibilityType(CompatibilityType compatibilityType) {
        this.compatibilityType = compatibilityType;
    }

    public String getErrDetail() {
        return errDetail;
    }

    public void setErrDetail(String errDetail) {
        this.errDetail = errDetail;
    }

    public String getOriginFileName() {
        return file;
    }

    public void setOriginFileName(String originFileName) {
        this.file = originFileName;
    }
}