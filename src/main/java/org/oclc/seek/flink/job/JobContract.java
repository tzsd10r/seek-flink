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
package org.oclc.seek.flink.job;

import java.io.Serializable;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public interface JobContract extends Serializable {
    /**
     * @param env
     * @throws Exception
     */
    public void execute(StreamExecutionEnvironment env) throws Exception;

    /**
     * @param env
     * @throws Exception
     */
    public void execute(ExecutionEnvironment env) throws Exception;

    /**
     *
     */
    public void init();
}
