package org.opengauss.assessment.dao;

public class SQLCompatibility {
    private int line;
    private String sql;
    private AssessmentType sqlType;
    private CompatibilityType compatibilityType;
    private String errDetail;

    public SQLCompatibility(int line, String sql, AssessmentType sqlType, CompatibilityType compatibilityType, String errDetail) {
        this.line = line;
        this.sql = sql;
        this.sqlType = sqlType;
        this.compatibilityType = compatibilityType;
        this.errDetail = errDetail;
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
}