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

import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

import org.opengauss.parser.FilesOperation;
import org.opengauss.parser.sqlparser.SqlParseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Description: parse sql statement from mybatis mapper file
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class MapperParser extends FileInputSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapperParser.class);

    private Configuration configuration;
    private XMLMapperBuilder mapperParser;
    private Collection<MappedStatement> mappedStatements;
    private File file;
    private Set<String> idSet;

    /**
     * Constructor
     */
    public MapperParser() {
    }

    /**
     * Constructor
     *
     * @param file File
     */
    public MapperParser(File file) {
        this.file = file;
    }

    /**
     * implement the run method in Runnable
     */
    @Override
    public void run() {
        parseSql();
    }

    /**
     * parse sql from single mybatis mapper file
     */
    public void parseSql() {
        if (this.file == null) {
            LOGGER.error("mapperParser: file is null");
            return;
        }
        parseSql(file);
    }

    /**
     * parse sql from file
     *
     * @param file File
     */
    protected void parseSql(File file) {
        File newFile = getOutputFile(file, outputDir, FileInputSqlParser.MAPPER_CODE);
        StringBuilder builder = new StringBuilder();
        if (!FilesOperation.isCreateOutputFile(newFile, outputDir)) {
            LOGGER.warn("create outputFile failed, it may already exists! inputfile: " + file.getName());
        }
        try (FileInputStream inputStream = new FileInputStream(file);
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(newFile, false)) {
            configuration = new Configuration();
            mapperParser = new XMLMapperBuilder(
                    inputStream, configuration, file.getCanonicalPath(), configuration.getSqlFragments());
            mapperParser.parse();
            idSet = new HashSet<>();
            mappedStatements = configuration.getMappedStatements();
            for (MappedStatement ms : mappedStatements) {
                idSet.add(ms.getId());
            }

            for (String id : idSet) {
                MappedStatement mappedStatement = configuration.getMappedStatement(id);
                SqlSource sqlSource = mappedStatement.getSqlSource();
                BoundSql boundSql = sqlSource.getBoundSql(null);
                String sql = boundSql.getSql();
                if (SqlParseController.isNeedFormat(sql)) {
                    builder.append(SqlParseController.format(sql));
                } else {
                    String formatSql = sql.replaceAll(SqlParseController.DELICRLF, ";").trim();
                    builder.append(formatSql.endsWith(";") ? formatSql + System.lineSeparator()
                            : formatSql + ';' + System.lineSeparator());
                }
            }
            SqlParseController.writeSqlToFile(newFile.getName(), bufWriter, builder);
        } catch (IOException | BuilderException | NullPointerException exp) {
            handleFileLockWhenExp(newFile.getName());
            LOGGER.error("create mapper file inputstream occur IOException or BuilderException"
                    + " or NullPointerException. filename: " + file.getName());
        }
    }
}
