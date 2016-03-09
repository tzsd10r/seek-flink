package org.oclc.seek.flink.socket
import org.junit.Before
import org.junit.Test
import org.oclc.seek.flink.job.impl.SocketToConsoleJob

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
class SocketToConsoleITest {
    SocketToConsoleJob socketToConsoleJob

    @Before
    public void setup( ) {
        socketToConsoleJob = new SocketToConsoleJob()
    }

    @Test
    public void testSocketToHdfs() {
        //socketToConsoleJob.execute(true)
        IHello h = new Hello()
        println h.name
        println h.first
    }

    public interface IHello {
        public String name = "El Seabra"
    }

    public class Hello implements IHello {
        public String first = "El"
        public String last = "Seabra"
    }
}
