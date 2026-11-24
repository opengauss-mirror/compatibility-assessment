/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * transcribe-replay-tool is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.kit.collect.exception.handler;

import lombok.extern.slf4j.Slf4j;
import org.kit.collect.exception.ParamsException;
import org.kit.collect.utils.response.RespBean;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * GlobalExceptionHandler
 *
 * @author liu
 * @since 2022-10-01
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    /**
     * globalException
     *
     * @param exception exception
     * @return RespBean
     */
    @ExceptionHandler(Exception.class)
    @ResponseBody
    public RespBean globalException(Exception exception) {
        log.error("globalException-->{}", exception.getMessage());
        return RespBean.error("occer exception", exception);
    }

    /**
     * paramsExceptionHandler
     *
     * @param exception exception
     * @return RespBean
     */
    @ExceptionHandler(ParamsException.class)
    @ResponseBody
    public RespBean paramsExceptionHandler(ParamsException exception) {
        return RespBean.error(exception.getMessage());
    }
}
