/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.record;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public interface EntityContract extends Serializable {
    /**
     * @return an instance of {@link EntityConfiguration}
     */
    // public EntityConfiguration configuration();

    /**
     *
     */
    // public interface EntityConfiguration extends Serializable {
    // /**
    // * @return the class name of the db driver
    // */
    // public String driver();
    //
    // /**
    // * @return a password for the db
    // */
    // public String password();
    //
    // /**
    // * @return a connection url for the db
    // */
    // public String url();
    //
    // /**
    // * @return a user name
    // */
    // public String user();
    //
    // /**
    // * @return a list of fields/column names
    // */
    // public List<String> fields();
    //
    // /**
    // * @return a table name
    // */
    // public String table();
    // }

    /**
     *
     */
    public interface EntityRecord extends Serializable {

        /**
         * @return a JSON representation of the record
         */
        public String toJson();
    }
}
