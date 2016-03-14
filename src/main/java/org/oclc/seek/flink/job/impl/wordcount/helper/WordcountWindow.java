/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl.wordcount.helper;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 */
public class WordcountWindow {
    /**
     *
     */
    public static class WordcountGlobalWindow implements
    WindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Long>, Tuple, GlobalWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(final Tuple key, final GlobalWindow window,
            final Iterable<Tuple2<String, Integer>> values,
            final Collector<Tuple3<String, Integer, Long>> collector) throws Exception {

            Long max = window.maxTimestamp();

            collector.collect(new Tuple3<>(key.toString(), Iterables.size(values), max));
        }
    }

    /**
     *
     */
    public static class WordcountTimeWindow implements
    WindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Long>, Tuple, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(final Tuple key, final TimeWindow window, final Iterable<Tuple2<String, Integer>> values,
            final Collector<Tuple3<String, Integer, Long>> collector) throws Exception {

            Long start = window.getStart();
            Long end = window.getEnd();
            Long elapsed = end - start;

            Long max = window.maxTimestamp();

            collector.collect(new Tuple3<>(key.toString(), Iterables.size(values), elapsed));
        }
    }
}
