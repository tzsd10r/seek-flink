/****************************************************************************************************************
 *
 *  Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 *
 *  OCLC proprietary information: the enclosed materials contain
 *  proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 *  any part to any third party or used by any person for any purpose, without written
 *  consent of OCLC, Inc.  Duplication of any portion of these  materials shall include his notice.
 *
 ******************************************************************************************************************/
package org.oclc.seek.flink.socket;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 */
public class DerbyInstance {
    private static Connection conn;

    /**
     *
     */
    public static void setup() {
        try {
            prepareDerbyDatabase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void prepareDerbyDatabase() throws ClassNotFoundException, SQLException {
        // System.setProperty("derby.stream.error.field",
        // "org.apache.flink.api.java.record.io.jdbc.DevNullLogStream.DEV_NULL");
        String dbURL = "jdbc:derby:memory:ebookshop;create=true";
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        conn = DriverManager.getConnection(dbURL);
        createTable();
        insertDataToSQLTable();
        conn.close();
    }

    private static void createTable() throws SQLException {
        StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE books (");
        sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
        sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
        sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
        sqlQueryBuilder.append("qty INT DEFAULT NULL,");
        sqlQueryBuilder.append("PRIMARY KEY (id))");

        Statement stat = conn.createStatement();
        stat.executeUpdate(sqlQueryBuilder.toString());
        stat.close();
    }

    private static void insertDataToSQLTable() throws SQLException {
        StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
        sqlQueryBuilder.append("(1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11),");
        sqlQueryBuilder.append("(1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22),");
        sqlQueryBuilder.append("(1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33),");
        sqlQueryBuilder.append("(1004, 'A Cup of Java', 'Kumar', 44.44, 44),");
        sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

        Statement stat = conn.createStatement();
        stat.execute(sqlQueryBuilder.toString());
        stat.close();
    }

    /**
     *
     */
    public static void teardown() {
        cleanUpDerbyDatabases();
    }

    private static void cleanUpDerbyDatabases() {
        try {
            String dbURL = "jdbc:derby:memory:ebookshop;create=true";
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

            conn = DriverManager.getConnection(dbURL);
            Statement stat = conn.createStatement();
            stat.executeUpdate("DROP TABLE books");
            stat.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
