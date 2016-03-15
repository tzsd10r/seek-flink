/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl.wordcount.helper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Returns a normalized instance of the value
 */
public class WordcountTokenizer {
    /**
     *
     */
    public static class WordcountStringToTupleTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final String value, final Collector<Tuple2<String, Integer>> out) throws Exception {
            /*
             * Normalize and split each line
             */
            String[] tokens = value.toLowerCase().split("\\W+");
            /*
             * Iterate through array of tokens and emit pairs containing: (string, 1), in Tuple2 format
             */
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, new Integer(1)));
                }
            }
        }
    }

    /**
     * emit the pairs with non-zero-length words
     */
    public static class WordcountArrayToTupleTokenizer implements FlatMapFunction<String[], Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final String[] values, final Collector<Tuple2<String, Integer>> collector) throws Exception {
            /*
             * Stream array of values and filter out invalid strings... and for each valid string emit pairs
             * containing: (string, 1), in Tuple2 format
             */
            // Arrays.stream(values).filter(new Predicate<String>() {
            // @Override
            // public boolean test(final String value) {
            // return value.length() > 0;
            // }
            // }).forEach(new Consumer<String>() {
            //
            // @Override
            // public void accept(final String value) {
            // collector.collect(new Tuple2<>(value, 1));
            // }
            // });
        }
    }

    /**
     *
     */
    public static class WordcountStringToArrayTokenizer implements MapFunction<String, String[]> {
        private static final long serialVersionUID = 1L;

        @Override
        public String[] map(final String value) throws Exception {
            /*
             * Normalize and split each line
             */
            return value.toLowerCase().split("\\W+");
        }
    }
}
