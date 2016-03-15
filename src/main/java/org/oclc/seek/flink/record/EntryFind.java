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

import java.util.Date;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.gson.Gson;

/**
 *
 */
public class EntryFind implements BaseObject {
    private static final long serialVersionUID = 1L;

    /** Non Indexed fields */
    private Long optimisticLock;

    private String userOclcnums;

    private String id;

    private String workId;

    private Date createdDate;

    private Date updatedDate;

    private String uid;

    private Long ownerInstitution;

    private Long sourceInstitution;

    private String collectionUid;

    private String status;

    private String oclcnum;

    private String issn;

    private String eissn;

    private String isbn;

    private String title;

    private String publisher;

    private String url;

    private String author;

    private String jkey;

    private String bkey;

    private String jsid;

    private String pubtype;

    private String coverage;

    private String coverageenum;

    private String note;

    private String coverageNote;

    private String location;

    private String isbns;

    private String userOclcnum;

    private String ext;

    private String collectionName;

    private String providerUid;

    private String providerName;

    private String scrubTitle;

    private String oclcnums;

    private String matchwsExecuted;

    private String issnl;

    private String content;

    private Long coverageStart;

    private Long coverageEnd;

    private Long volumeStart;

    private Long volumeEnd;

    private Long issueStart;

    private Long issueEnd;

    private String open;

    private String openAccess;

    private String holdingsRegid;

    private String holdingsInstid;

    /** default constructor */
    public EntryFind() {
        super();
    }

    /**
     * get id
     *
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * set id
     *
     * @param idFind
     *            id
     */
    public void setId(final String idFind) {
        id = idFind;
    }

    /**
     * get optimisticLock
     *
     * @return optimisticLock
     */
    public Long getOptimisticLock() {
        return optimisticLock;
    }

    /**
     * set optimisticLock
     *
     * @param optimisticLockFind
     *            optimisticLock
     */
    public void setOptimisticLock(final Long optimisticLockFind) {
        optimisticLock = optimisticLockFind;
    }

    /**
     * get createdDate
     *
     * @return createdDate
     */
    public Date getCreatedDate() {
        return createdDate;
    }

    /**
     * set createdDate
     *
     * @param createdDateFind
     *            createdDate
     */
    public void setCreatedDate(final Date createdDateFind) {
        createdDate = createdDateFind;

    }

    /**
     * get updatedDate
     *
     * @return updatedDate
     */
    public Date getUpdatedDate() {
        return updatedDate;
    }

    /**
     * set updatedDate
     *
     * @param updatedDateFind
     *            updatedDate
     */
    public void setUpdatedDate(final Date updatedDateFind) {
        updatedDate = updatedDateFind;
    }

    /**
     * get ownerInstitution
     *
     * @return ownerInstitution
     */
    public Long getOwnerInstitution() {
        return ownerInstitution;
    }

    /**
     * set ownerInstitution
     *
     * @param ownerInstitutionFind
     *            ownerInstitution
     */
    public void setOwnerInstitution(final Long ownerInstitutionFind) {
        ownerInstitution = ownerInstitutionFind;
    }

    /**
     * get sourceInstitution
     *
     * @return sourceInstitution
     */
    public Long getSourceInstitution() {
        return sourceInstitution;
    }

    /**
     * set sourceLibrary
     *
     * @param sourceLibrary
     *            sourceLibrary
     */
    public void setSourceInstitution(final Long sourceLibrary) {
        sourceInstitution = sourceLibrary;
    }

    /**
     * get collectionUid
     *
     * @return collectionUid
     */
    public String getCollectionUid() {
        return collectionUid;
    }

    /**
     * set collectionUid
     *
     * @param collectionUidFind
     *            collectionUid
     */
    public void setCollectionUid(final String collectionUidFind) {
        collectionUid = collectionUidFind;
    }

    /**
     * get uid
     *
     * @return uid
     */
    public String getUid() {
        return uid;
    }

    /**
     * set uid
     *
     * @param uidFind
     *            uid
     */
    public void setUid(final String uidFind) {
        uid = uidFind;
    }

    /**
     * get oclcnum
     *
     * @return oclcnum
     */
    public String getOclcnum() {
        return oclcnum;
    }

    /**
     * set oclcnum
     *
     * @param oclcnumFind
     *            oclcnum
     */
    public void setOclcnum(final String oclcnumFind) {
        oclcnum = oclcnumFind;
    }

    /**
     * get issn
     *
     * @return issn
     */
    public String getIssn() {
        return issn;
    }

    /**
     * set issn
     *
     * @param issnFind
     *            issn
     */
    public void setIssn(final String issnFind) {
        issn = issnFind;
    }

    /**
     * get eissn
     *
     * @return eissn
     */
    public String getEissn() {
        return eissn;
    }

    /**
     * set eissn
     *
     * @param eissnFind
     *            eissn
     */
    public void setEissn(final String eissnFind) {
        eissn = eissnFind;
    }

    /**
     * get status
     *
     * @return status
     */
    public String getStatus() {
        return status;
    }

    /**
     * set status
     *
     * @param status
     *            status
     */
    public void setStatus(final String status) {
        this.status = status;
    }

    /**
     * get isbn
     *
     * @return isbn
     */
    public String getIsbn() {
        return isbn;
    }

    /**
     * set isbn
     *
     * @param isbnFind
     *            isbn
     */
    public void setIsbn(final String isbnFind) {
        isbn = isbnFind;
    }

    /**
     * get title
     *
     * @return title
     */
    public String getTitle() {
        return title;
    }

    /**
     * set title
     *
     * @param titleFind
     *            title
     */
    public void setTitle(final String titleFind) {
        title = titleFind;
    }

    /**
     * get publisher
     *
     * @return publisher
     */
    public String getPublisher() {
        return publisher;
    }

    /**
     * set publisher
     *
     * @param publisherFind
     *            publisher
     */
    public void setPublisher(final String publisherFind) {
        publisher = publisherFind;
    }

    /**
     * get url
     *
     * @return url
     */
    public String getUrl() {
        return url;
    }

    /**
     * set url
     *
     * @param urlFind
     *            url
     */
    public void setUrl(final String urlFind) {
        url = urlFind;
    }

    /**
     * get author
     *
     * @return author
     */
    public String getAuthor() {
        return author;
    }

    /**
     * set author
     *
     * @param authorFind
     *            author
     */
    public void setAuthor(final String authorFind) {
        author = authorFind;
    }

    /**
     * get jkey
     *
     * @return jkey
     */
    public String getJkey() {
        return jkey;
    }

    /**
     * set jkey
     *
     * @param jkeyFind
     *            jkey
     */
    public void setJkey(final String jkeyFind) {
        jkey = jkeyFind;
    }

    /**
     * get bkey
     *
     * @return bkey
     */
    public String getBkey() {
        return bkey;
    }

    /**
     * set bkey
     *
     * @param bkeyFind
     *            bkey
     */
    public void setBkey(final String bkeyFind) {
        bkey = bkeyFind;
    }

    /**
     * get jsid
     *
     * @return jsid
     */
    public String getJsid() {
        return jsid;
    }

    /**
     * set jsid
     *
     * @param jsidFind
     *            jsid
     */
    public void setJsid(final String jsidFind) {
        jsid = jsidFind;
    }

    /**
     * get pubtype
     *
     * @return pubtype
     */
    public String getPubtype() {
        return pubtype;
    }

    /**
     * set pubtype
     *
     * @param pubtypeFind
     *            pubtype
     */
    public void setPubtype(final String pubtypeFind) {
        pubtype = pubtypeFind;
    }

    /**
     * get coverage
     *
     * @return coverage
     */
    public String getCoverage() {
        return coverage;
    }

    /**
     * set coverage
     *
     * @param coverageFind
     *            coverage
     */
    public void setCoverage(final String coverageFind) {
        coverage = coverageFind;
    }

    /**
     * get coverageenum
     *
     * @return coverageenum
     */
    public String getCoverageenum() {
        return coverageenum;
    }

    /**
     * set coverageenum
     *
     * @param coverageenumFind
     *            coverageenum
     */
    public void setCoverageenum(final String coverageenumFind) {
        coverageenum = coverageenumFind;
    }

    /**
     * get note
     *
     * @return note
     */
    public String getNote() {
        return note;
    }

    /**
     * set note
     *
     * @param noteFind
     *            note
     */
    public void setNote(final String noteFind) {
        note = noteFind;
    }

    /**
     * Gets the coverage note.
     *
     * @return the coverageNote
     */
    public String getCoverageNote() {
        return coverageNote;
    }

    /**
     * Sets the coverage note.
     *
     * @param coverageNote
     *            the coverageNote to set
     */
    public void setCoverageNote(final String coverageNote) {
        this.coverageNote = coverageNote;
    }

    /**
     * Gets the location.
     *
     * @return the location
     */
    public String getLocation() {
        return location;
    }

    /**
     * Sets the location.
     *
     * @param location
     *            the location to set
     */
    public void setLocation(final String location) {
        this.location = location;
    }

    /**
     * Gets the isbns.
     *
     * @return the isbns
     */
    public String getIsbns() {
        return isbns;
    }

    /**
     * Sets the isbns.
     *
     * @param isbns
     *            the isbns to set
     */
    public void setIsbns(final String isbns) {
        this.isbns = isbns;
    }

    /**
     * Gets the user oclcnum.
     *
     * @return the userOclcnum
     */
    public String getUserOclcnum() {
        return userOclcnum;
    }

    /**
     * Sets the user oclcnum.
     *
     * @param userOclcnum
     *            the userOclcnum to set
     */
    public void setUserOclcnum(final String userOclcnum) {
        this.userOclcnum = userOclcnum;
    }

    /**
     * Gets the user oclcnums.
     *
     * @return the userOclcnums
     */
    public String getUserOclcnums() {
        return userOclcnums;
    }

    /**
     * Sets the user oclcnums.
     *
     * @param userOclcnums
     *            the userOclcnums to set
     */
    public void setUserOclcnums(final String userOclcnums) {
        this.userOclcnums = userOclcnums;
    }

    /**
     * get ext
     *
     * @return ext
     */
    public String getExt() {
        return ext;
    }

    /**
     * set ext
     *
     * @param ext
     *            ext
     */
    public void setExt(final String ext) {
        this.ext = ext;

    }

    /**
     * get collectionName
     *
     * @return collectionName
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * set collectionName
     *
     * @param collectionNameFind
     *            collectionName
     */
    public void setCollectionName(final String collectionNameFind) {
        collectionName = collectionNameFind;
    }

    /**
     * get providerUid
     *
     * @return providerUid
     */
    public String getProviderUid() {
        return providerUid;
    }

    /**
     * set providerUid
     *
     * @param providerUidFind
     *            providerUid
     */
    public void setProviderUid(final String providerUidFind) {
        providerUid = providerUidFind;
    }

    /**
     * get providerName
     *
     * @return providerName
     */
    public String getProviderName() {
        return providerName;
    }

    /**
     * set providerName
     *
     * @param providerNameFind
     *            providerName
     */
    public void setProviderName(final String providerNameFind) {
        providerName = providerNameFind;
    }

    /**
     * get scrubTitle
     *
     * @return scrubTitle
     */
    public String getScrubTitle() {
        return scrubTitle;
    }

    /**
     * set scrubTitle
     *
     * @param scrubTitleFind
     *            scrubTitle
     */
    public void setScrubTitle(final String scrubTitleFind) {
        scrubTitle = scrubTitleFind;
    }

    /**
     * get oclcnums
     *
     * @return oclcnums
     */
    public String getOclcnums() {
        return oclcnums;
    }

    /**
     * whether matchws is executed
     *
     * @return the matchwsExecuted
     */
    public String getMatchwsExecuted() {
        return matchwsExecuted;
    }

    /**
     * set matchwsExectuted status
     *
     * @param matchwsExecuted
     *            the matchwsExecuted to set
     */
    public void setMatchwsExecuted(final String matchwsExecuted) {
        this.matchwsExecuted = matchwsExecuted;
    }

    /**
     * set oclcnums
     *
     * @param oclcnumsFind
     *            oclcnums
     */
    public void setOclcnums(final String oclcnumsFind) {
        oclcnums = oclcnumsFind;
    }

    /**
     * get issnl
     *
     * @return issnl
     */
    public String getIssnl() {
        return issnl;
    }

    /**
     * set issnl
     *
     * @param issnlFind
     *            issnl
     */
    public void setIssnl(final String issnlFind) {
        issnl = issnlFind;
    }

    /**
     * get workId
     *
     * @return workId
     */
    public String getWorkId() {
        return workId;
    }

    /**
     * set workId
     *
     * @param workIdFind
     *            workId
     */
    public void setWorkId(final String workIdFind) {
        workId = workIdFind;
    }

    /**
     * get content
     *
     * @return content
     */
    public String getContent() {
        return content;
    }

    /**
     * set content
     *
     * @param contentFind
     *            content
     */
    public void setContent(final String contentFind) {
        content = contentFind;
    }

    /**
     * get coverageStart
     *
     * @return coverageStart
     */
    public Long getCoverageStart() {
        return coverageStart;
    }

    /**
     * set coverageStart
     *
     * @param coverageStartFind
     *            coverageStart
     */
    public void setCoverageStart(final Long coverageStartFind) {
        coverageStart = coverageStartFind;
    }

    /**
     * get coverageEnd
     *
     * @return coverageEnd
     */
    public Long getCoverageEnd() {
        return coverageEnd;
    }

    /**
     * set coverageEnd
     *
     * @param coverageEndFind
     *            coverageEnd
     */
    public void setCoverageEnd(final Long coverageEndFind) {
        coverageEnd = coverageEndFind;
    }

    /**
     * get volumeStart
     *
     * @return volumeStart
     */
    public Long getVolumeStart() {
        return volumeStart;
    }

    /**
     * set volumeStart
     *
     * @param volumeStartFind
     *            volumeStart
     */
    public void setVolumeStart(final Long volumeStartFind) {
        volumeStart = volumeStartFind;
    }

    /**
     * get volumeEnd
     *
     * @return volumeEnd
     */
    public Long getVolumeEnd() {
        return volumeEnd;
    }

    /**
     * set volumeEnd
     *
     * @param volumeEndFind
     *            volumeEnd
     */
    public void setVolumeEnd(final Long volumeEndFind) {
        volumeEnd = volumeEndFind;
    }

    /**
     * get issueStart
     *
     * @return issueStart
     */
    public Long getIssueStart() {
        return issueStart;
    }

    /**
     * set issueStart
     *
     * @param issueStartFind
     *            issueStart
     */
    public void setIssueStart(final Long issueStartFind) {
        issueStart = issueStartFind;
    }

    /**
     * get issueEnd
     *
     * @return issueEnd
     */
    public Long getIssueEnd() {
        return issueEnd;
    }

    /**
     * set issueEnd
     *
     * @param issueEndFind
     *            issueEnd
     */
    public void setIssueEnd(final Long issueEndFind) {
        issueEnd = issueEndFind;
    }

    /**
     * get open status
     *
     * @return open status
     */
    public String getOpen() {
        return open;
    }

    /**
     * set open status
     *
     * @param openFind
     *            open status
     */
    public void setOpen(final String openFind) {
        open = openFind;
    }

    /**
     * get open access status
     *
     * @return openAccess status
     */
    public String getOpenAccess() {
        return openAccess;
    }

    /**
     * set open access status
     *
     * @param openAccessFind
     *            open access status
     */
    public void setOpenAccess(final String openAccessFind) {
        openAccess = openAccessFind;
    }

    /**
     * get holdingsRegid
     *
     * @return holdingsRegid
     */
    public String getHoldingsRegid() {
        return holdingsRegid;
    }

    /**
     * set holdingsRegid
     *
     * @param holdingsRegidFind
     *            holdingsRegid
     */
    public void setHoldingsRegid(final String holdingsRegidFind) {
        holdingsRegid = holdingsRegidFind;
    }

    /**
     * set holdings Regid as a set
     *
     * @param holdingRegid
     *            list of regid
     */

    public void putHoldingsRegidBySet(final TreeSet<String> holdingRegid) {
        setHoldingsRegid(StringUtils.join(holdingRegid, " "));
    }

    /**
     * get holdingsInstid
     *
     * @return holdingsInstid
     */
    public String getHoldingsInstid() {
        return holdingsInstid;
    }

    /**
     * set holdingsInstid
     *
     * @param holdingsInstidFind
     *            holdingsInstid
     */
    public void setHoldingsInstid(final String holdingsInstidFind) {
        holdingsInstid = holdingsInstidFind;
    }

    /**
     * set holdings Instid as a set
     *
     * @param holdingInstId
     *            list of institution id
     */
    public void putHoldingsInstidBySet(final TreeSet<String> holdingInstId) {
        setHoldingsInstid(StringUtils.join(holdingInstId, " "));
    }

    /**
     * get string
     *
     * @return string
     */
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    /**
     * @param json
     * @return an json record
     */
    public EntryFind fromJson(final String json) {
        return new Gson().fromJson(json, this.getClass());
    }

    /**
     * @return
     */
    public String toJson() {
        String s = new Gson().toJson(this);
        return s;
    }
}
