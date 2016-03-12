/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.builder;

import org.oclc.seek.flink.record.BaseObject;

import com.google.gson.Gson;

/**
 * @param <R>
 */
public class DocumentBuilder<R> implements DocumentBuilderContract<R> {
    private static final long serialVersionUID = 1L;

    /**
     * @param input
     * @return an instance of type R
     */
    @Override
    public R build(final BaseObject input, final Class<R> clazz) {
        if (input == null) {
            return null;
        }

        Gson g = new Gson();
        String json = g.toJson(input);

        return g.fromJson(json, clazz);
    }

    // @SuppressWarnings("unchecked")
    // private static <T> T build(final GenericRecord input, final Class type) {
    // if (input == null) {
    // return null;
    // }
    // Gson g = new Gson();
    // String s = g.toJson(input);
    // return (T) g.fromJson(s, type);
    // }
    //
    // public static <T> List<T> build(final List<? extends GenericRecord> efs, final T instance) {
    // if (efs == null) {
    // return null;
    // }
    // Gson g = new Gson();
    // String s = g.toJson(efs);
    // Type collectionType = new TypeToken<List<T>>() {
    // }.getType();
    // return g.fromJson(s, collectionType);
    // // return g.fromJson(s, instance.getClass());
    // }

}
