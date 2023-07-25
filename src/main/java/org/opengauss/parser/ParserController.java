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

package org.opengauss.parser;

import org.opengauss.parser.entry.CommandEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: sql parser module entry class
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class ParserController implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParserController.class);

    private String[] args;

    /**
     * get
     *
     * @return String[]
     */
    public String[] getArgs() {
        return args;
    }

    /**
     * set
     *
     * @param args String[]
     */
    public void setArgs(String[] args) {
        this.args = args;
    }

    private void startParser() {
        CommandEntry commandEntry = new CommandEntry();
        commandEntry.mainEntry(args);
    }

    /**
     * implement the run method in Runnable
     */
    public void run() {
        startParser();
    }
}
