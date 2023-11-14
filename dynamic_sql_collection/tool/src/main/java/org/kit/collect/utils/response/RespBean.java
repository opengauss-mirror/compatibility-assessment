/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RespBean
 *
 * @author liu
 * @since 2023-09-17
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RespBean {
    private Integer code;

    private String message;

    @JsonInclude(Include.NON_NULL)
    private Object obj;

    /**
     * 成功返回结果
     *
     * @param message message
     * @return RespBean RespBean
     */
    public static RespBean success(String message) {
        return new RespBean(200, message, null);
    }

    /**
     * 成功返回结果
     *
     * @param message message
     * @param obj obj
     * @return RespBean RespBean
     */
    public static RespBean success(String message, Object obj) {
        return new RespBean(200, message, obj);
    }

    /**
     * 失败返回结果
     *
     * @param message message
     * @return RespBean RespBean
     */
    public static RespBean error(String message) {
        return new RespBean(500, message, null);
    }

    /**
     * 失败返回结果
     *
     * @param message message
     * @param  obj obj
     * @return RespBean RespBean
     */
    public static RespBean error(String message, Object obj) {
        return new RespBean(500, message, obj);
    }
}