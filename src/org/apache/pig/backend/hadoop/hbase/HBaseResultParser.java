/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.backend.hadoop.hbase;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.*;

/**
 * Incapsulates parsing logic for {@link Result}.
 * Parser takes care of {@link HBaseStorage#loadRowKey_} and {@link HBaseStorage#includeCellTimestamp_} flags
 *
 */

@Data
public class HBaseResultParser {

    private static final Log LOG = LogFactory.getLog(HBaseResultParser.class);

    private final boolean includeCellTimeStamp;
    List<HBaseStorage.ColumnInfo> columnInfo;
    private final boolean loadRowKey;
    private final int tupleSize;

    @Builder
    /**
     * @param loadRowKey false by default
     * @param columnInfo is metadata for column families and qualifier filters used to get data from HBase. Mandatory
     * @param includeCellTimeStamp is false by default
     * */
    public HBaseResultParser(boolean loadRowKey, @NonNull List<HBaseStorage.ColumnInfo> columnInfo, boolean includeCellTimeStamp) {
        this.loadRowKey = loadRowKey;
        this.columnInfo = columnInfo;
        this.includeCellTimeStamp = includeCellTimeStamp;
        this.tupleSize = columnInfo.size() + (loadRowKey? 1: 0) + (includeCellTimeStamp? columnInfo.size() : 0);
    }

    public Tuple parse(RecordReader reader) throws IOException, InterruptedException {
        Tuple tuple = TupleFactory.getInstance().newTuple(getTupleSize());

        if(loadRowKey) {
            ImmutableBytesWritable rowKey = (ImmutableBytesWritable) reader.getCurrentKey();
            tuple.set(0, new DataByteArray(rowKey.get()));
        }

        Result result = (Result) reader.getCurrentValue();
        ListIterator<HBaseStorage.ColumnInfo> columnItr = columnInfo.listIterator();
        while(columnItr.hasNext()){
            HBaseStorage.ColumnInfo columnInfo = columnItr.next();
            int itrPreviousIndex = columnItr.previousIndex();
            if(columnInfo.isColumnMap()){
                addColumnFamilyMap(result, columnInfo, tuple, itrPreviousIndex);
            }else{
                addValue(result, columnInfo, tuple, itrPreviousIndex);
            }
        }
        return tuple;
    }

    private void addColumnFamilyMap(Result result, HBaseStorage.ColumnInfo columnInfo, Tuple tuple, int itrPreviousIndex) throws ExecException {
        // useful feature.
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result.getNoVersionMap();

        NavigableMap<byte[], byte[]> cfResults = resultsMap.get(columnInfo.getColumnFamily());
        Map<String, DataByteArray> cfMap = new HashMap<String, DataByteArray>();
        if (cfResults != null) {
            for (byte[] quantifier : cfResults.keySet()) {
                // We need to check against the prefix filter to
                // see if this value should be included. We can't
                // just rely on the server-side filter, since a
                // user could specify multiple CF filters for the
                // same CF.
                if (columnInfo.getColumnPrefix() == null ||
                        columnInfo.hasPrefixMatch(quantifier)) {

                    byte[] cell = cfResults.get(quantifier);
                    DataByteArray value =
                            cell == null ? null : new DataByteArray(cell);
                    cfMap.put(Bytes.toString(quantifier), value);
                }
            }
        }
        if(includeCellTimeStamp){
            Map<String, Long> timestampMap = new HashMap<String, Long>();
            if (cfResults != null) {
                for (byte[] quantifier : cfResults.keySet()) {
                    timestampMap.put(Bytes.toString(quantifier), getTimestamp(result, columnInfo, quantifier));
                }
            }
            tuple.set(getTimestampIndex(itrPreviousIndex), timestampMap);
        }

        tuple.set(getValueIndex(itrPreviousIndex), cfMap);

    }

    private void addValue(Result result, HBaseStorage.ColumnInfo columnInfo, Tuple tuple, int itrPreviousIndex) throws ExecException {
        byte[] cell = result.getValue(columnInfo.getColumnFamily(), columnInfo.getColumnName());
        DataByteArray value = cell == null ? null : new DataByteArray(cell);
        if(includeCellTimeStamp){
            Long ts = getTimestamp(result, columnInfo);
            tuple.set(getTimestampIndex(itrPreviousIndex), ts);
        }
        tuple.set(getValueIndex(itrPreviousIndex), value);
    }

    private int getTimestampIndex(int itrPreviousIndex){
        return itrPreviousIndex + (loadRowKey? 1: 0) + (includeCellTimeStamp? itrPreviousIndex : 0);
    }

    private int getValueIndex(int itrPreviousIndex){
        return itrPreviousIndex + (includeCellTimeStamp ? 1 : 0);
    }


    private Long getTimestamp(Result result, HBaseStorage.ColumnInfo columnInfo, byte[] quantifier){
        KeyValue keyValue = result.getColumnLatest(columnInfo.getColumnFamily(), quantifier);
        return keyValue == null ? null : keyValue.getTimestamp();
    }

    private Long getTimestamp(Result result, HBaseStorage.ColumnInfo columnInfo){
        KeyValue keyValue = result.getColumnLatest(columnInfo.getColumnFamily(), columnInfo.getColumnName());
        return keyValue == null ? null : keyValue.getTimestamp();
    }

}
