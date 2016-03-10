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
package org.oclc.seek.flink.builder;

import org.oclc.seek.flink.record.DbInputRecord;

public class DbInputRecordBuilder {
    private DbInputRecord dbInputRecord;

    public DbInputRecordBuilder() {
        dbInputRecord = new DbInputRecord();
    }

    public DbInputRecord build() {
        return dbInputRecord;
    }

    public DbInputRecordBuilder ownerInstitution(final Long ownerInstitution) {
        // dbInputRecord.setOwnerInstitution(ownerInstitution);
        return this;
    }

    public DbInputRecordBuilder collectionUid(final String collectionUid) {
        // dbInputRecord.setCollectionUid(collectionUid);
        return this;
    }
}