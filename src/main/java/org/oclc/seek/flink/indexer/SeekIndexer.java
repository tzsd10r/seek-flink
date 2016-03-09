
package org.oclc.seek.flink.indexer;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class SeekIndexer implements Serializable {
    private static final long serialVersionUID = 1L;

    private String database;
    private long size;
    private long elapsed;

    public SeekIndexer(final String db) {
        database = db;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(final String database) {
        this.database = database;
    }

    public long getSize() {
        return size;
    }

    public void setSize(final long size) {
        this.size = size;
    }

    public long getElapsed() {
        return elapsed;
    }

    public void setElapsed(final long e) {
        elapsed = e;
    }

    /**
     * get string
     *
     * @return string
     */
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
