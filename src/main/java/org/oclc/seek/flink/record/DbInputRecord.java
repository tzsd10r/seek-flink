/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.record;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

/**
 *
 */
public class DbInputRecord extends GenericRecord {
    private String driver;
    private String url;
    private String user;
    private String password;
    private String table;

    private String[] fields = new String[] {
        "owner_institution", "collection_uid"
    };

    private Long ownerInstitution;
    private String collectionUid;
    private Map<String, Object> map = new HashMap<String, Object>();

    @Override
    public void readFields(final ResultSet rs) throws SQLException {
        setOwnerInstitution(rs.getLong("owner_institution"));
        setCollectionUid(rs.getString("collection_uid"));
    }

    public void setOwnerInstitution(final Long ownerInstitution) {
        this.ownerInstitution = ownerInstitution;
        map.put(fields[0], ownerInstitution);
    }

    public void setCollectionUid(final String collectionUid) {
        this.collectionUid = collectionUid;
        map.put(fields[1], collectionUid);
    }

    /**
     * @return
     */
    public String toJson() {
        String s = new Gson().toJson(map);
        // System.out.println(s);
        return s;
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
    public String table() {
        return table;
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
