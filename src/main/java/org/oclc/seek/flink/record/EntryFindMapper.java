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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


/**
 *
 */
public class EntryFindMapper {
    /**
     * @param rs
     * @return
     * @throws SQLException
     */
    public static EntryFind mapRow(final ResultSet rs) throws SQLException {
        EntryFind ef = new EntryFind();

        ef.setId(rs.getString("ID"));
        ef.setOptimisticLock(rs.getLong("OPTIMISTIC_LOCK"));
        Timestamp cd = rs.getTimestamp("CREATED_DATE");
        if (cd!=null) {
            ef.setCreatedDate(cd);
        }
        Timestamp ud = rs.getTimestamp("UPDATED_DATE");
        if (ud!=null) {
            ef.setUpdatedDate(ud);
        }
        ef.setOwnerInstitution(rs.getLong("OWNER_INSTITUTION"));
        ef.setSourceInstitution(rs.getLong("SOURCE_INSTITUTION"));
        ef.setCollectionUid(rs.getString("COLLECTION_UID"));
        ef.setCollectionName(rs.getString("COLLECTION_NAME"));
        ef.setProviderUid(rs.getString("PROVIDER_UID"));
        ef.setProviderName(rs.getString("PROVIDER_NAME"));
        ef.setUid(rs.getString("UID"));
        ef.setStatus(rs.getString("STATUS"));
        ef.setOclcnum(rs.getString("OCLCNUM"));
        ef.setOclcnums(rs.getString("OCLCNUMS"));
        ef.setMatchwsExecuted(rs.getString("MATCHWS_EXECUTED"));
        ef.setIssn(rs.getString("ISSN"));
        ef.setEissn(rs.getString("EISSN"));
        ef.setIssnl(rs.getString("ISSNL"));
        ef.setIsbn(rs.getString("ISBN"));
        ef.setWorkId(rs.getString("WORKID"));
        ef.setTitle(rs.getString("TITLE"));
        ef.setScrubTitle(rs.getString("SCRUBTITLE"));

        ef.setPublisher(rs.getString("PUBLISHER"));
        ef.setUrl(rs.getString("URL"));
        ef.setAuthor(rs.getString("AUTHOR"));
        ef.setContent(rs.getString("CONTENT"));
        ef.setJkey(rs.getString("JKEY"));
        ef.setBkey(rs.getString("BKEY"));
        ef.setJsid(rs.getString("JSID"));
        ef.setPubtype(rs.getString("PUBTYPE"));
        ef.setCoverage(rs.getString("COVERAGE"));
        ef.setCoverageStart(rs.getLong("COVERAGE_START"));
        ef.setCoverageEnd(rs.getLong("COVERAGE_END"));
        ef.setCoverageenum(rs.getString("COVERAGEENUM"));

        ef.setVolumeStart(rs.getLong("VOLUME_START"));
        ef.setVolumeEnd(rs.getLong("VOLUME_END"));
        ef.setIssueStart(rs.getLong("ISSUE_START"));
        ef.setIssueEnd(rs.getLong("ISSUE_END"));
        ef.setOpenAccess(rs.getString("OPENACCESS"));
        ef.setOpen(rs.getString("OPEN"));
        ef.setNote(rs.getString("NOTE"));
        ef.setExt(rs.getString("EXT"));
        ef.setHoldingsRegid(rs.getString("HOLDINGS_REGID"));
        ef.setHoldingsInstid(rs.getString("HOLDINGS_INSTID"));
        ef.setCoverageNote(rs.getString("COVERAGE_NOTE"));

        ef.setLocation(rs.getString("LOCATION"));
        ef.setIsbns(rs.getString("ISBNS"));
        ef.setUserOclcnum(rs.getString("USER_OCLCNUM"));
        ef.setUserOclcnums(rs.getString("USER_OCLCNUMS"));

        return ef;
    }
}
