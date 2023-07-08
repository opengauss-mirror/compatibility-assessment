package org.opengauss.assessment.dao;

public class DB_CompatibilityAttr {
    private int flag;
    private String name;

    public DB_CompatibilityAttr(int flag, String name) {
        this.flag = flag;
        this.name = name;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}