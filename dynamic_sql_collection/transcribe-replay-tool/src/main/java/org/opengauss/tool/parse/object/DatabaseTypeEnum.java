/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

package org.opengauss.tool.parse.object;

import org.opengauss.tool.parse.ParseThread;

/**
 * Database type enum
 *
 * @since 2024-11-09
 */
public enum DatabaseTypeEnum {
    /**
     * MySQL
     */
    MYSQL {
        @Override
        public ParseThread getSuitableProtocolParser(String clientId) {
            return new ParseThread(clientId);
        }
    };

    /**
     * Gets suitable protocol parse thread
     *
     * @param clientId String the client id ip:port
     *
     * @return ParseThread the suitable database protocol parse thread
     */
    public abstract ParseThread getSuitableProtocolParser(String clientId);

    /**
     * Gets enum instance according database type
     *
     * @param databaseType String the database type
     *
     * @return database type enum instance
     */
    public static DatabaseTypeEnum fromTypeName(String databaseType) {
        for (DatabaseTypeEnum type : values()) {
            if (type.name().equalsIgnoreCase(databaseType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported database type: " + databaseType);
    }
}
