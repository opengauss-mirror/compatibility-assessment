/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
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

package org.opengauss.tool.replay.factory;

import org.opengauss.tool.config.DatabaseConfig;
import org.opengauss.tool.utils.ConnectionFactory;
import org.opengauss.tool.utils.DatabaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ReplayConnectionFactory
 *
 * @since 2024-07-01
 */
public class ReplayConnectionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayConnectionFactory.class);
    private static final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();

    private static ReplayConnectionFactory instance;

    private ReplayConnectionFactory() {
    }

    /**
     * get replayConnectionFactory instance
     *
     * @return replayConnectionFactory
     */
    public static synchronized ReplayConnectionFactory getInstance() {
        if (instance == null) {
            instance = new ReplayConnectionFactory();
        }
        return instance;
    }

    /**
     * get replay connection
     *
     * @param targetDbConfig targetDbConfig
     * @param schema         schema
     * @param schemaMap      schemaMap
     * @return replay connection
     */
    public Optional<Connection> getConnection(DatabaseConfig targetDbConfig, String schema,
                                              Map<String, String> schemaMap) {
        if (!connectionMap.containsKey(Thread.currentThread().getName() + schema)) {
            if (!schemaMap.containsKey(schema)) {
                return Optional.empty();
            }
            targetDbConfig.setDbName(schemaMap.get(schema));
            Connection connection = ConnectionFactory.createConnection(targetDbConfig, ConnectionFactory.OPENGAUSS);
            setSessionConfig(connection);
            connectionMap.put(Thread.currentThread().getName() + schema, connection);
        }
        return Optional.of(connectionMap.get(Thread.currentThread().getName() + schema));
    }

    private void setSessionConfig(Connection connection) {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.execute("set b_format_behavior_compat_options='enable_multi_charset'");
            stmt.execute("set bytea_output = escape");
            stmt.execute("set session_timeout to 0");
        } catch (SQLException e) {
            LOGGER.error("set 'b_format_behavior_compat_options' failed, error message:{}", e.getMessage());
        } finally {
            DatabaseOperator.closeStatement(stmt);
        }
    }

    /**
     * update connection map
     *
     * @param connectionKey connectionKey
     * @param replayConn    replayConn
     */
    public void updateOgConnectionMap(String connectionKey, Connection replayConn) {
        connectionMap.put(connectionKey, replayConn);
    }
}
