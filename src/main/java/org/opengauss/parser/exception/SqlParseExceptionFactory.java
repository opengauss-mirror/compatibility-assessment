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

package org.opengauss.parser.exception;

/**
 * Description: sql parse exception factory
 *
 * @author jianghongbo
 * @since 2023/7/24
 */
public class SqlParseExceptionFactory {
    /**
     * commandline exception code
     */
    public static final int CMDLINEEXCEPTION_CODE = 1;

    /**
     * configuration exception code
     */
    public static final int CONFIGUREEXCEPTION_CODE = 2;

    /**
     * load configure file exception code
     */
    public static final int LOADFILEEXCEPTION_CODE = 3;

    /**
     * sql parse exception code
     */
    public static final int PARSEEXCEPTION_CODE = 4;

    /**
     * Exception factory, generate exception by exception code
     *
     * @param expCode int
     * @param message String
     * @return RuntimeException
     */
    public static RuntimeException getException(final int expCode, String message) {
        RuntimeException exp = null;
        if (expCode == CMDLINEEXCEPTION_CODE) {
            exp = new CommandLineArgsException(message);
        }
        if (expCode == CONFIGUREEXCEPTION_CODE) {
            exp = new ConfigureArgsException(message);
        }
        if (expCode == LOADFILEEXCEPTION_CODE) {
            exp = new LoadFileException(message);
        }
        if (expCode == PARSEEXCEPTION_CODE) {
            exp = new SqlParserException(message);
        }
        return exp;
    }
}
