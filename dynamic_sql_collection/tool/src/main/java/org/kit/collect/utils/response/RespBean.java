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