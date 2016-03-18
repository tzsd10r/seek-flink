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
package org.oclc.seek.flink.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 *
 */
public class GenericWritable implements Writable, DBWritable {
    @Override
    public void readFields(final ResultSet arg0) throws SQLException {
    }

    @Override
    public void write(final PreparedStatement arg0) throws SQLException {
    }

    @Override
    public void readFields(final DataInput arg0) throws IOException {
    }

    @Override
    public void write(final DataOutput arg0) throws IOException {
    }

}
