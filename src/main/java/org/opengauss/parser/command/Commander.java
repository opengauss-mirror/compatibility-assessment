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

package org.opengauss.parser.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;

import org.opengauss.parser.exception.SqlParseExceptionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Description: Parse commandline args
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class Commander {
    /**
     * report file extension
     */
    public static final String REPORTFILE_EXTENSION = "html";

    /**
     * command line parameter, get sql by collect
     */
    public static final String DATAFROM_COLLECT = "collect";

    /**
     * command line parameter, get sql from file
     */
    public static final String DATAFROM_FILE = "file";
    private static final Logger LOGGER = LoggerFactory.getLogger(Commander.class);
    private static final String DATASOURCE_OPTION_FULL = "dataSource";
    private static final String DATASOURCE_OPTION = "d";
    private static final String DATASOURCE_DESCRIPTION = "-d or --dataSource [file | collect]";
    private static final String CONFFILE_OPTION_FULL = "confFile";
    private static final String CONFFILE_OPTION = "c";
    private static final String CONFFILE_DESCRIPTION = "-c or --confFile conffilepath";
    private static final String OUTPUT_OPTION_FULL = "output";
    private static final String OUTPUT_OPTION = "o";
    private static final String OUTPUT_DESCRIPTION = "-o or --output outputpath";
    private static String reportFile = System.getProperty("user.dir") + File.separator + "report.html";
    private static String confFile = null;
    private static String dataSource = null;
    private static volatile boolean isParseComplete = false;

    private CommandLineParser commandlineParser;
    private Options options;

    /**
     * Constructor
     */
    public Commander() {
        commandlineParser = new DefaultParser();
        options = new Options();
        addOptions();
    }

    /**
     * get confFile
     *
     * @return String
     */
    public static String getConfFile() {
        return confFile;
    }

    /**
     * get report file
     *
     * @return String
     */
    public static String getReportFile() {
        return reportFile;
    }

    /**
     * get data from source
     *
     * @return String
     */
    public static String getDataSource() {
        return dataSource;
    }

    /**
     * parse commandline args
     *
     * @param args String[]
     */
    public void parseCmd(String[] args) {
        CommandLine commandline;
        try {
            commandline = commandlineParser.parse(options, args);
        } catch (ParseException exp) {
            LOGGER.error("fatal: ParseException occured!", exp);
            throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CMDLINEEXCEPTION_CODE,
                    "ParseException occured! exp: " + exp.getMessage());
        }
        setDataSource(commandline);
        setConfFile(commandline);
        setReportFile(commandline);
        isParseComplete = true;
    }

    public static boolean isCommanderParseComplete() {
        return isParseComplete;
    }

    private void addOptions() {
        options.addOption(DATASOURCE_OPTION, DATASOURCE_OPTION_FULL, true, DATASOURCE_DESCRIPTION);
        options.addOption(CONFFILE_OPTION, CONFFILE_OPTION_FULL, true, CONFFILE_DESCRIPTION);
        options.addOption(OUTPUT_OPTION, OUTPUT_OPTION_FULL, true, OUTPUT_DESCRIPTION);
    }

    private void setConfFile(CommandLine commandline) {
        if (commandline.hasOption(CONFFILE_OPTION)) {
            confFile = commandline.getOptionValue(CONFFILE_OPTION);
        }
    }

    private void setDataSource(CommandLine commandline) {
        if (commandline.hasOption(DATASOURCE_OPTION)) {
            String datafrom = commandline.getOptionValue(DATASOURCE_OPTION);
            if (!datafrom.equalsIgnoreCase(DATAFROM_COLLECT)
                    && !datafrom.equalsIgnoreCase(DATAFROM_FILE)) {
                LOGGER.error("fatal: -d must be specified by \'file\' or \'collect\'");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CMDLINEEXCEPTION_CODE,
                        "commandline parameter -d specify a incorrect value!");
            }
            dataSource = datafrom;
        } else {
            LOGGER.error("fatal: commandline args incomplete!");
            throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CMDLINEEXCEPTION_CODE,
                    "commandline args incomplete!");
        }
    }

    private void setReportFile(CommandLine commandline) {
        if (commandline.hasOption(OUTPUT_OPTION)) {
            if (!checkReportFile(commandline.getOptionValue(OUTPUT_OPTION))) {
                LOGGER.error("report file must be .html");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CMDLINEEXCEPTION_CODE,
                        "report file must be .html");
            }
            reportFile = commandline.getOptionValue(OUTPUT_OPTION);
        }
    }

    private boolean checkReportFile(String filename) {
        return REPORTFILE_EXTENSION.equals(FilenameUtils.getExtension(filename));
    }
}
