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
 * Assessment controller.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class AssessmentController implements Runnable {
    /**
     * Start assessment.
     */
    public static void startAssessment() {
        AssessmentEntry assessmentEntry = new AssessmentEntry();
        assessmentEntry.assessment();
    }

    /**
     * Thread execution Body.
     */
    @Override
    public void run() {
        startAssessment();
    }
}