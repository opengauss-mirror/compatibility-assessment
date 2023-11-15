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

package org.opengauss.parser.sqlparser.fileinputsqlparser;

import org.opengauss.parser.sqlparser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Description: parse all types of files in dataDir at the same time
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class FilesSqlParser implements SqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilesSqlParser.class);
    private static final int MAX_THREAD_NUM = 20;

    private List<File> sqlfiles;
    private List<File> generallogs;
    private List<File> slowlogs;
    private List<File> mappers;
    private ThreadPoolExecutor poolExecutor;


    /**
     * Constructor
     *
     * @param sqlfiles List<File>
     * @param generallogs List<File>
     * @param slowlogs List<File>
     * @param mappers List<File>
     */
    public FilesSqlParser(List<File> sqlfiles, List<File> generallogs, List<File> slowlogs, List<File> mappers) {
        this.sqlfiles = sqlfiles;
        this.generallogs = generallogs;
        this.slowlogs = slowlogs;
        this.mappers = mappers;
        initThreadPool();
    }

    /**
     * parse all types of files, each type of file is parsed using a thread pool
     */
    @Override
    public void parseSql() {
        for (File sqlfile : sqlfiles) {
            poolExecutor.execute(new SqlFileParser(sqlfile));
        }
        for (File generalLog : generallogs) {
            poolExecutor.execute(new GeneralLogParser(generalLog));
        }
        for (File slowLog : slowlogs) {
            poolExecutor.execute(new SlowLogParser(slowLog));
        }
        for (File mapper : mappers) {
            poolExecutor.execute(new MapperParser(mapper));
        }
        poolExecutor.shutdown();
    }

    private void initThreadPool() {
        int poolSize = generallogs.size() + slowlogs.size() + mappers.size() + sqlfiles.size();
        poolExecutor = new ThreadPoolExecutor(Math.min(poolSize + 1, MAX_THREAD_NUM), MAX_THREAD_NUM,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }
}
