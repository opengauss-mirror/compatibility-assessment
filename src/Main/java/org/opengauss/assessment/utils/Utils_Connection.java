package org.opengauss.assessment.utils;

import org.opengauss.parser.configure.ConfigureInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Utils_Connection {

    public static Connection getConnection(String dbname) {
        String host = ConfigureInfo.getConfigureInfo().getOgInfo().getHost();
        String port = ConfigureInfo.getConfigureInfo().getOgInfo().getPort();
        String user = ConfigureInfo.getConfigureInfo().getOgInfo().getUser();
        String password = ConfigureInfo.getConfigureInfo().getOgInfo().getPassword();
        String url = "jdbc:opengauss://" + host + ":" + port + "/" + dbname + "?useUnicode=false&characterEncoding=utf8&usessL=false";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return connection;
    }

    public static void getConnectionClose(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}