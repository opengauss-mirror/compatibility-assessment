package org.opengauss.assessment.dao;

public class ScanSingleSql {
    private String sql;
    private int line;

    public ScanSingleSql(String sql, int line) {
        this.sql = sql;
        this.line = line;
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
}