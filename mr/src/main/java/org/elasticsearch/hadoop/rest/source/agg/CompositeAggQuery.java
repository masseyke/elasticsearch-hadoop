/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.rest.source.agg;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.query.QueryBuilder;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.CompositeAggReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CompositeAggQuery<T> implements Iterator<T>, Closeable, StatsAware {

    private final RestRepository repository;
    private final CompositeAggReader<T> reader;
    private final String endpoint;
    private final QueryBuilder queryBuilder;
    private final List<String> groupByFields;
    private final List<CompositeAggReader.AggInfo> aggregates;
    private final Stats stats = new Stats();

    private BytesArray afterKey;
    private String currentPitId;
    private List<T> batch = Collections.emptyList();
    private boolean finished = false;
    private int batchIndex = 0;
    private long read = 0;
    private boolean closed = false;
    private boolean initialized = false;

    public CompositeAggQuery(RestRepository client, String endpoint, String pitId, QueryBuilder query,
                             List<String> groupByFields, List<CompositeAggReader.AggInfo> aggregates, CompositeAggReader<T> reader) {
        this.repository = client;
        this.reader = reader;
        this.endpoint = endpoint;
        this.currentPitId = pitId;
        this.queryBuilder = query;
        this.groupByFields = groupByFields;
        this.aggregates = aggregates;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            finished = true;
            batch = Collections.emptyList();
            reader.close();
            // typically the scroll is closed after it is consumed so this will trigger a 404
            // however we're closing it either way
            if (StringUtils.hasText(currentPitId)) {
                repository.getRestClient().deletePointInTime(currentPitId);
            }
            repository.close();
        }
    }

    @Override
    public boolean hasNext() {
        if (finished) {
            return false;
        }

        if (!initialized) {
            initialized = true;

            try {
                nextBatch();
            } catch (IOException ex) {
                throw new EsHadoopIllegalStateException(String.format("Cannot create aggregation for query [%s]", endpoint), ex);
            }
        }

        while (!finished && (batch.isEmpty() || batchIndex >= batch.size())) {
            // TODO: Fix when we can put the after key into the agg request
            finished = true; break;
//            try {
//                nextBatch();
//            } catch (IOException ex) {
//                throw new EsHadoopIllegalStateException("Cannot retrieve scroll [" + afterKey + "]", ex);
//            }
//            read += batch.size();
//            stats.docsReceived += batch.size();
//
//            // reset index
//            batchIndex = 0;
        }

        return !finished;
    }

    private void nextBatch() throws IOException {
        if (groupByFields.isEmpty()) {
            FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
            JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
            try {
                generator.writeBeginObject();
                {
                    generator.writeFieldName("size").writeNumber(0);
                    // Query
                    generator.writeFieldName("query").writeBeginObject();
                    {
                        queryBuilder.toJson(generator);
                    }
                    generator.writeEndObject();
                    // Aggs
                    {
                        generator.writeFieldName("aggs").writeBeginObject();
                        for (CompositeAggReader.AggInfo aggregate : aggregates) {
                            generator.writeFieldName(aggregate.getFieldKey()).writeBeginObject()
                                    .writeFieldName(aggregate.getAggType()).writeBeginObject()
                                    .writeFieldName("field").writeString(aggregate.getFieldName())
                                    .writeEndObject()
                                    .writeEndObject();
                        }
                        generator.writeEndObject();
                    }
                }
                generator.writeEndObject();
            } finally {
                generator.close();
            }
            CompositeAggReader.CompositeAgg<T> composite = repository.aggregateStream(endpoint, out.bytes(), reader);
            batch = composite.getResult().getRows();
            finished = false;
        } else {
            // Create request body
            // TODO: Maybe we can reuse parts of the bulk request templating api here?
            FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
            JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
            try {
                generator.writeBeginObject();
                {
                    generator.writeFieldName("size").writeNumber(0);
                    generator.writeFieldName("track_total_hits").writeBoolean(false);
                    // Query
                    generator.writeFieldName("query").writeBeginObject();
                    {
                        queryBuilder.toJson(generator);
                    }
                    generator.writeEndObject();
                    // Point in Time
                    generator.writeFieldName("pit").writeBeginObject();
                    {
                        generator.writeFieldName("id").writeString(currentPitId);
                        generator.writeFieldName("keep_alive").writeString("5m"); // TODO Make configurable
                    }
                    generator.writeEndObject();
                    // Aggs
                    generator.writeFieldName("aggs").writeBeginObject();
                    {
                        generator.writeFieldName("COMPOSITE").writeBeginObject(); // TODO Field name configurable? Helpful?
                        {
                            generator.writeFieldName("composite").writeBeginObject();
                            {
                                generator.writeFieldName("size").writeNumber(10000); // TODO Configurable
                                generator.writeFieldName("sources").writeBeginArray();
                                for (String groupByField : groupByFields) {
                                    generator.writeBeginObject()
                                            .writeFieldName(groupByField).writeBeginObject()
                                            .writeFieldName("terms").writeBeginObject()
                                            .writeFieldName("field").writeString(groupByField)
                                            .writeEndObject()
                                            .writeEndObject()
                                            .writeEndObject();
                                }
                                generator.writeEndArray();
//                            if (afterKey != null) {
//                                // TODO: Generators can't take a regular chunk of already made JSON and add it to the
//                                //  stream. This needs to be changed in order to paginate the data coming in.
//                                generator.writeFieldName("after_key");
//                                // add after_key contents to generator
//                            }
                            }
                            generator.writeEndObject();
                            generator.writeFieldName("aggs").writeBeginObject();
                            for (CompositeAggReader.AggInfo aggregate : aggregates) {
                                generator.writeFieldName(aggregate.getFieldKey()).writeBeginObject()
                                        .writeFieldName(aggregate.getAggType()).writeBeginObject()
                                        .writeFieldName("field").writeString(aggregate.getFieldName())
                                        .writeEndObject()
                                        .writeEndObject();
                            }
                            generator.writeEndObject();
                        }
                        generator.writeEndObject();
                    }
                    generator.writeEndObject();
                }
                generator.writeEndObject();
            } finally {
                generator.close();
            }

            CompositeAggReader.CompositeAgg<T> composite = repository.aggregateStream(endpoint, out.bytes(), reader);
            currentPitId = composite.getPitId();
            afterKey = composite.getResult().getAfterKey();
            batch = composite.getResult().getRows();
            finished = composite.getResult().isConcluded();
        }
    }

    public long getRead() {
        return read;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more documents available");
        }
        return batch.get(batchIndex++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("read-only operator");
    }

    @Override
    public Stats stats() {
        // there's no need to do aggregation
        return new Stats(stats);
    }

    public RestRepository repository() {
        return repository;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompositeAggQuery [scrollId=").append(afterKey).append("]");
        return builder.toString();
    }
}