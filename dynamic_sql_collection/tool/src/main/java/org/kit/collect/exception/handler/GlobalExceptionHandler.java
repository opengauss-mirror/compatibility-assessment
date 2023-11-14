/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
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
