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

package org.opengauss.parser.configure;

/**
 * Description: configure info management interface
 *
 * @author jianghongbo
 * @since 2023/7/17
 */
public interface ConfigureInfoManager {
    /**
     * get property by key first and second
     *
     * @param first String
     * @param second String
     * @return String
     */
    String getProperty(String first, String second);

    /**
     * get property by key first and second
     *
     * @param key String
     * @return String
     */
    String getProperty(String key);
}
