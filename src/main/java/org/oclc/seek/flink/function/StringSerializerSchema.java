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
package org.oclc.seek.flink.function;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 *
 */
public class StringSerializerSchema implements DeserializationSchema<String>, SerializationSchema<String, byte[]> {
    // public class StringSerializerSchema implements DeserializationSchema<String>, SerializationSchema<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(final byte[] message) {
        return new String(message);
    }

    @Override
    public boolean isEndOfStream(final String nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(final String element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}