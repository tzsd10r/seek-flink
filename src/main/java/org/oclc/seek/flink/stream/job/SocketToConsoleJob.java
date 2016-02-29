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
package org.oclc.seek.flink.stream.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;

/**
 *
 */
public class SocketToConsoleJob extends JobGeneric implements JobContract {

    @Override
    public void init() {
    }

    @Override
    public void execute() throws Exception {
        execute(false);
    }

    /**
     * @param local
     * @throws Exception
     */
    public void execute(final boolean local) throws Exception {
        StreamExecutionEnvironment env;
        if (local) {
            env = StreamExecutionEnvironment.createLocalEnvironment();
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        DataStream<Tuple2<String, Integer>> stream = env.socketTextStream("localhost", 8989)
            .flatMap(new Splitter()).keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        stream.print();

        env.execute("Window WordCountJob");
    }

    /**
     *
     */
    public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final String sentence, final Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        new SocketToConsoleJob().execute(false);
    }
}
