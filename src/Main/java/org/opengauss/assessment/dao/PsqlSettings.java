package org.opengauss.assessment.dao;

public class PsqlSettings {
    private String proname;
    private String dbname;

    public PsqlSettings() {
    }

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