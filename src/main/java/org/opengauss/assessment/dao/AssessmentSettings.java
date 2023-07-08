package org.opengauss.assessment.dao;

public class AssessmentSettings {
    private String dbname;
    private boolean needCreateDatabase = false;
    private boolean plugin = false;
    private boolean extension = false;
    /* input database type */
    private int database = -1;
    private String inputDir = null;
    private String outputFile = null;

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public boolean isNeedCreateDatabase() {
        return needCreateDatabase;
    }

    public void setNeedCreateDatabase(boolean needCreateDatabase) {
        this.needCreateDatabase = needCreateDatabase;
    }

    public boolean isPlugin() {
        return plugin;
    }

    public void setPlugin(boolean plugin) {
        this.plugin = plugin;
    }

    public boolean isExtension() {
        return extension;
    }

    public void setExtension(boolean extension) {
        this.extension = extension;
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

    public AssessmentSettings() {
    }
}