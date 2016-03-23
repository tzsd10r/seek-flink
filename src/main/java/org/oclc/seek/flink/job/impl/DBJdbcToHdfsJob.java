/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.source.JDBCInputFormat;
import org.oclc.seek.flink.source.JDBCSource;

import com.google.gson.Gson;

/**
 * This class is still in construction... does not work!!!!
 */
public class DBJdbcToHdfsJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final ExecutionEnvironment env) throws Exception {
        // make configuration available globally in all functions
        env.getConfig().setGlobalJobParameters(parameterTool);
        
//        TupleTypeInfo<Tuple2<String, String>> tupleTypeInfo = new TupleTypeInfo<Tuple2<String, String>>(BasicTypeInfo.LONG_TYPE_INFO,
//            BasicTypeInfo.STRING_TYPE_INFO);
//        
//        JDBCInputFormat<Tuple2<String, String>> source = new JDBCSource<Tuple2<String, String>>(parameterTool);
//        source.open().getResulSet();
//        
//        DataSet<Tuple2<String, String>> dbRecords = env.createInput(source, tupleTypeInfo);

        /*
         * Flink's program compiler needs to infer the data types of the data items which are returned
         * by an InputFormat. If this information cannot be automatically inferred, it is necessary to
         * manually provide the type information as shown below.
         */
        TupleTypeInfo<Tuple2<Long, String>> tupleTypeInfo = new TupleTypeInfo<Tuple2<Long, String>>(BasicTypeInfo.LONG_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);
        
        JDBCInputFormat<Tuple2<Long, String>> source = new JDBCSource<Tuple2<Long, String>>(parameterTool);
        
        source.open().getResulSet();
        
        DataSet<Tuple2<Long, String>> dbRecords = env.createInput(source, tupleTypeInfo);

        DataSet<String> jsonRecords = dbRecords.flatMap(new Mapper());

        String path = parameterTool.get("fs.sink.dir." + parameterTool.get("db.table"));
        jsonRecords.writeAsText(path + "/entry-find.txt", WriteMode.OVERWRITE)
        .name("filesystem sink");

        env.execute("Fetch Data from Database and write to filesystem sink");
    }
    
    public class Mapper extends RichFlatMapFunction<Tuple2<Long, String>, String> {
        private static final long serialVersionUID = 1L;
        private String[] fields;


        @Override
        public void open(final Configuration parameters) throws Exception {
            fields = parameters.getString("fields", null).split(",");
        }

        @Override
        public void flatMap(final Tuple2<Long, String> tuple, final Collector<String> output) throws Exception {
            Map<String, Object> map = new HashMap<String, Object>();

            String k0 = fields[0].trim();
            String k1 = fields[1].trim();

            Object v0 = tuple.f0;
            Object v1 = tuple.f1;

            Tuple2<String, Object>[] tupleArray = new Tuple2Builder<String, Object>()
                .add(k0, v0)
                .add(k1, v1)
                .build();

            for (Tuple2<String, Object> t : tupleArray) {
                map.put(t.f0, t.f1);
            }

            output.collect(new Gson().toJson(map));
        }
    }
}
