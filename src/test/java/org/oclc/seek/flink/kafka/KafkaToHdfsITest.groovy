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
package org.oclc.seek.flink.kafka

import org.junit.Before
import org.junit.Test
import org.oclc.seek.flink.stream.job.KafkaToHdfsJob

class KafkaToHdfsITest {
    KafkaToHdfsJob kafkaToHdfsJob

    @Before
    public void setup() {
        kafkaToHdfsJob = new KafkaToHdfsJob('conf/config.ilab.properties')
    }

    @Test
    public void testKafkaToHdfs() {
        kafkaToHdfsJob.execute()
    }
}
