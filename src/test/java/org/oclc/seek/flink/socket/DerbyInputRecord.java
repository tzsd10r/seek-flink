/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.socket;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.oclc.seek.flink.record.GenericWritable;

/**
 *
 */
public class DerbyInputRecord extends GenericWritable {
    private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    private String url = "jdbc:derby:memory:ebookshop";
    private String user = "";
    private String password = "";
    private String table = "books";
    private String[] fields = new String[] {"id", "title", "author", "price", "qty"};

    private int id;
    private String title;
    private String author;
    private double price;
    private int qty;


    /**
     *
     */
    public DerbyInputRecord() {
        DerbyInstance.setup();
    }

    @Override
    public void readFields(final ResultSet rs) throws SQLException {
        id = rs.getInt(1);
        title = rs.getString(2);
        author = rs.getString(3);
        price = rs.getDouble(4);
        qty = rs.getInt(5);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(id);
        s.append("\036");
        s.append(title);
        s.append("\036");
        s.append(author);
        s.append("\036");
        s.append(price);
        s.append("\036");
        s.append(qty);
        s.append("\036");

        return s.toString();
    }

    /**
     * @return
     */
    public String[] fields() {
        return fields;
    }

    /**
     * @return
     */
    public String driver() {
        return driver;
    }

    /**
     * @return
     */
    public String url() {
        return url;
    }

    /**
     * @return
     */
    public String table() {
        return table;
    }

    /**
     * @return
     */
    public String user() {
        return user;
    }

    /**
     * @return
     */
    public String password() {
        return password;
    }
}
