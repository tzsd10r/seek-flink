/****************************************************************************************************************
 * Copyright (c) 2015 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.mapper;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 
 */
public class DBFetcherUtility {

    public static JdbcTemplate createPoolableJdbcTemplate(ParameterTool parameterTool) {
        String driver = parameterTool.getRequired("db.driver");
        String url = parameterTool.getRequired("db.url");
        String user = parameterTool.getRequired("db.user");
        String password = parameterTool.getRequired("db.password");

        BasicDataSource datasource = new BasicDataSource();
        datasource.setDriverClassName(driver);
        datasource.setUsername(user);
        datasource.setPassword(password);
        datasource.setUrl(url);
        datasource.setDefaultQueryTimeout(7200); // 120 minutes
        datasource.setEnableAutoCommitOnReturn(false);
        datasource.setMaxTotal(100);
        datasource.setMaxIdle(6);
        datasource.setTestWhileIdle(true);
        datasource.setValidationQuery("SELECT 1");
        datasource.setTestOnBorrow(true);
        datasource.setMinEvictableIdleTimeMillis(120000); // 2 minutes
        datasource.setTimeBetweenEvictionRunsMillis(120000); // 2 minutes
        datasource.setMinIdle(2);

        return new JdbcTemplate(datasource);
    }

    public static JdbcTemplate createJdbcTemplate(ParameterTool parameterTool) {
        String url = parameterTool.getRequired("db.url");
        String user = parameterTool.getRequired("db.user");
        String password = parameterTool.getRequired("db.password");
        
        return new JdbcTemplate(new DriverManagerDataSource(url, user, password));
    }

}
