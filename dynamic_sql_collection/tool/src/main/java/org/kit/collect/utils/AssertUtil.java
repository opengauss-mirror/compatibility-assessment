/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils;

import org.kit.collect.exception.ParamsException;

/**
 * AssertUtil
 *
 * @author liu
 * @since 2023-09-17
 */
public class AssertUtil {
    /**
     * isTrue
     *
     * @param isFlag isFlag
     * @param msg msg
     */
    public static void isTrue(Boolean isFlag, String msg) {
        if (isFlag) {
            throw new ParamsException(msg);
        }
    }
}
