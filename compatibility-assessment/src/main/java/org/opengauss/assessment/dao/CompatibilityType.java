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
 * Compatibility type.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public enum CompatibilityType {
    COMPATIBLE, AST_COMPATIBLE, INCOMPATIBLE, UNSUPPORTED_COMPATIBLE, SKIP_COMMAND;

    /**
     * judge compatible
     *
     * @param sqlCompatibility : record assessment information.
     * @return boolean
     */
    public static boolean isCompatible(SQLCompatibility sqlCompatibility) {
        CompatibilityType compatibilityType = sqlCompatibility.getCompatibilityType();
        return compatibilityType == COMPATIBLE || compatibilityType == AST_COMPATIBLE;
    }

    /**
     * judge incompatible
     *
     * @param sqlCompatibility : record assessment information.
     * @return boolean
     */
    public static boolean isIncompatible(SQLCompatibility sqlCompatibility) {
        CompatibilityType compatibilityType = sqlCompatibility.getCompatibilityType();
        return compatibilityType == INCOMPATIBLE
                || compatibilityType == UNSUPPORTED_COMPATIBLE
                || compatibilityType == SKIP_COMMAND;
    }
}