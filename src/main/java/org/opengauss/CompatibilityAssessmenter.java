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

package org.opengauss;

import org.opengauss.assessment.dao.AssessmentController;
import org.opengauss.parser.ParserController;
import org.opengauss.parser.command.Commander;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

/**
 * Compatibility assessment entry.
 *
 * @author : yuchao
 * @since : in 2023/7/7
 */
public class CompatibilityAssessmenter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompatibilityAssessmenter.class);
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 10,
            10L, TimeUnit.SECONDS, new LinkedBlockingQueue(100));

    public static void main(String[] args) {
        sqlParsingThreadExecute(args);
        checkConditions();
        sqlAssessmentThreadExecute();
    }

    private static void checkConditions() {
        while (true) {
            if (Commander.isCommanderParseComplete()) {
                break;
            }
        }

        while (!AssessmentInfoManager.getInstance().getAssessmentFlag()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException exp) {
                LOGGER.warn("main thread occur InterruptedException");
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("output sqlfile number is : " + AssessmentInfoManager.getInstance().getOutputSqlFileCount());
        }
    }

    private static void sqlParsingThreadExecute(String[] args) {
        ParserController parserController = new ParserController();
        parserController.setArgs(args);
        Future future = threadPoolExecutor.submit(parserController);

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("sqlParsing thread occur Exception, exit. exception = " + e.getMessage());
            threadPoolExecutor.shutdownNow();
        }
    }

    private static void sqlAssessmentThreadExecute() {
        AssessmentController assessmentController = new AssessmentController();
        Future future = threadPoolExecutor.submit(assessmentController);

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("sqlPAssessment thread occur Exception, exit. exception = " + e.getMessage());
            threadPoolExecutor.shutdownNow();
        }

        if (!threadPoolExecutor.isShutdown()) {
            threadPoolExecutor.shutdown();
        }
    }
}