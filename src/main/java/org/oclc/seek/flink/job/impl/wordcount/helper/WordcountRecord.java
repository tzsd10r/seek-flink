/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl.wordcount.helper;

import java.io.Serializable;

/**
 *
 */
public class WordcountRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private String word;
    private Long processingTime;
    private Long count;

    public WordcountRecord(final String word, final Long processingTime, final Long count) {
        setWord(word);
        setProcessingTime(processingTime);
        setCount(count);
    }

    public String getWord() {
        return word;
    }

    public void setWord(final String word) {
        this.word = word;
    }

    public Long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(final Long processingTime) {
        this.processingTime = processingTime;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(final Long count) {
        this.count = count;
    }
}
