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

package org.opengauss.tool.replay.task;

import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.utils.ConnectionFactory;
import org.opengauss.tool.utils.DatabaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Replay Task from Database
 *
 * @since 2024-07-01
 */
public class ReplayDbTask extends ReplayMainTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayDbTask.class);
    private static final BlockingQueue<List<SqlModel>> sqlModelListQueue = new LinkedBlockingQueue<>(5);
    private static final int PAGINATION = 500;
    private static final int TIME_OUT_MILLIS = 5000;
    private static AtomicBoolean isReadEnd;

    /**
     * constructor
     *
     * @param replayConfig replayConfig
     */
    public ReplayDbTask(ReplayConfig replayConfig) {
        super(replayConfig);
        isReadEnd = new AtomicBoolean(false);
    }

    @Override
    public void replay() {
        try {
            Thread readThread = new Thread(this::readSql);
            readThread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("An uncaught exception occurred in readThread"
                    + t.getName() + e.getMessage()));
            readThread.start();
            while (!isReadEnd.get() || !sqlModelListQueue.isEmpty()) {
                List<SqlModel> sqlModelList = sqlModelListQueue.take();
                for (SqlModel sqlModel : sqlModelList) {
                    replaySql(sqlModel);
                }
            }
            LOGGER.info("readThread will be interrupt");
            readThread.interrupt();
        } catch (InterruptedException e) {
            LOGGER.error("replay sql from db has occurred an interruptedException, error message:{}", e.getMessage());
        }
    }

    private void readSql() {
        Connection storeConn = ConnectionFactory.createConnection(replayConfig.getSourceDbConfig(),
                ConnectionFactory.OPENGAUSS);
        String storageTableName = replayConfig.getSourceDbConfig().getTableName();
        try {
            int count = replaySqlOperator.getSqlCount(storeConn, storageTableName);
            ProcessModel processModel = ProcessModel.getInstance();
            processModel.addSqlCount(count);
            int pageCount = (int) Math.ceil((double) count / PAGINATION);
            readSqlFromDb(pageCount, storeConn, storageTableName, processModel);
        } catch (SQLException | InterruptedException e) {
            LOGGER.error("replay from db has occurred an exception:{}", e.getMessage());
        }
    }

    private void readSqlFromDb(int pageCount, Connection storeConn, String storageTableName, ProcessModel processModel)
            throws SQLException, InterruptedException {
        Connection connection = storeConn;
        for (int page = 0; page < pageCount; page++) {
            LOGGER.info("read sql from db, totalPage:{}, currentPage:{}", pageCount, page);
            List<SqlModel> sqlModels = new ArrayList<>();
            ResultSet rs = null;
            try {
                rs = replaySqlOperator.getSqlResultSet(connection, storageTableName, PAGINATION,
                        page + 1);
                while (rs.next()) {
                    SqlModel sqlModel = new SqlModel(rs, connection, storageTableName);
                    sqlModels.add(sqlModel);
                }
            } finally {
                DatabaseOperator.closeResultSet(rs);
            }
            long startTime = System.currentTimeMillis();
            sqlModelListQueue.put(sqlModels);
            long endTime = System.currentTimeMillis();
            if ((endTime - startTime) > TIME_OUT_MILLIS) {
                connection = ConnectionFactory.createConnection(replayConfig.getSourceDbConfig(),
                        ConnectionFactory.OPENGAUSS);
            }
            if (page == pageCount - 1) {
                isReadEnd.set(true);
                processModel.setReadFinish();
            }
        }
    }
}
