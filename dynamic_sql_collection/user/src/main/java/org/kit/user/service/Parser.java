/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.user.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * parser
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
@Component
public class Parser {
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException exception) {
            log.error(exception.getMessage());
        }
    }

    /**
     * starter
     *
     * @throws SQLException SQLException
     */
    @Scheduled(cron = "0 */1 * * * ?")
    public void starter() throws SQLException {
        try {
            Connection connection = DriverManager.getConnection(
                    "", "root", "1234");
            String sql3 = "INSERT INTO `sys_dict_type` VALUES (?, ?, ?, ?, ?,?, ?, ?, ?)";
            PreparedStatement prepared1 = connection.prepareStatement(sql3);
            prepared1.setString(1, "1");
            prepared1.setString(2, "用户性别");
            prepared1.setString(3, "sys_user_sex");
            prepared1.setString(4, "0");
            prepared1.setString(5, "admin");
            prepared1.setString(6, "2022-10-14 15:08:02");
            prepared1.setString(7, "null");
            prepared1.setString(8, "2022-10-14 15:08:03");
            prepared1.setString(9, "用户性别列表");
            String sql4 = "select dict_id, dict_name, dict_type, status, create_by, create_time, "
                    + "remark from sys_dict_type where dict_id = ?";
            PreparedStatement prepared2 = connection.prepareStatement(sql4);
            prepared2.setString(1, "1");
            prepared2.execute();
            Statement statement = connection.createStatement();
            String sql1 = "select dict_id, dict_name, dict_type, status, create_by, create_time, "
                    + "remark from sys_dict_type";
            statement.execute(sql1);
            String sql5 = "select dict_id, dict_name, create_by, create_time, remark from sys_dict_type";
            statement.executeQuery(sql5);
            String sql2 = "delete from sys_dict_type where dict_id in(1)";
            statement.executeUpdate(sql2);
            String sql6 = "update sys_dict_type set  status = 1 WHERE dict_id = 10;";
            statement.executeUpdate(sql6);
            prepared1.execute();
        } catch (SQLException exception) {
            throw new SQLException("sql error");
        }
    }
}
