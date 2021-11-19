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

package org.elasticsearch.hadoop.serialization;

import org.elasticsearch.hadoop.rest.EsHadoopParsingException;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.builder.ValueParsingCallback;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class CompositeAggReader<T> implements Closeable {

    public static class CompositeAgg<T> {
        private final String pitId;
        private final CompositeAggResult<T> result;

        public CompositeAgg(String pitId, CompositeAggResult<T> result) {
            this.pitId = pitId;
            this.result = result;
        }

        public String getPitId() {
            return pitId;
        }

        public CompositeAggResult<T> getResult() {
            return result;
        }
    }

    public static class CompositeAggResult<T> {
        private final BytesArray afterKey;
        private final List<T> rows;
        private final boolean concluded;

        public CompositeAggResult(BytesArray afterKey, List<T> rows, boolean concluded) {
            this.afterKey = afterKey;
            this.rows = rows;
            this.concluded = concluded;
        }

        public BytesArray getAfterKey() {
            return afterKey;
        }

        public List<T> getRows() {
            return rows;
        }

        public boolean isConcluded() {
            return concluded;
        }
    }

    public static class AggInfo implements Serializable {
        private final String fieldKey;
        private final String fieldName;
        private final FieldType fieldType;
        private final String aggType;

        public AggInfo(String fieldKey, String fieldName, FieldType fieldType, String aggType) {
            this.fieldKey = fieldKey;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.aggType = aggType;
        }

        public String getFieldKey() {
            return fieldKey;
        }

        public String getFieldName() {
            return fieldName;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        public String getAggType() {
            return aggType;
        }
    }

    private static final String FIELD_PIT_ID = "pit_id";
    private static final String FIELD_AGGREGATIONS = "aggregations";
    private static final String FIELD_AFTER_KEY = "after_key";
    private static final String FIELD_BUCKETS = "buckets";
    private static final String FIELD_KEY = "key";
    private static final String FIELD_DOC_COUNT = "doc_count";
    private static final String FIELD_VALUE = "value";

    private final String targetAggregationName = "COMPOSITE";

    private final ValueReader reader;
    private final ValueParsingCallback parsingCallback;
    private final Map<String, FieldType> esMapping;
    private final Optional<AggInfo> countStarAgg;
    private final Map<String, AggInfo> targetAggs;

    public CompositeAggReader(ValueReader reader, ValueParsingCallback parsingCallback, Map<String, FieldType> esMapping,
                              Optional<AggInfo> countStarAgg, Map<String, AggInfo> targetAggs) {
        this.reader = reader;
        this.parsingCallback = parsingCallback;
        this.esMapping = esMapping;
        this.countStarAgg = countStarAgg;
        this.targetAggs = targetAggs;
    }

    @Override
    public void close() {
        // Nothing needed for now
    }

    public CompositeAgg<T> read(InputStream content) throws IOException {
        Assert.notNull(content);

        // copy the content
        BytesArray copy = IOUtils.asBytes(content);
        content = new FastByteArrayInputStream(copy);

        // Create parser
        Parser parser = new JacksonJsonParser(content);

        try {
            return parseResponseBody(parser, copy);
        } finally {
            parser.close();
        }
    }

    private CompositeAgg<T> parseResponseBody(Parser parser, BytesArray copy) {
        Assert.notNull(parser);
        parser.nextToken(); // Advance to first token
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; invalid start of stream.");

        String pitId = null;
        CompositeAggResult<T> result = null;

        Token token = parser.nextToken();
        while (token != Token.END_OBJECT) {
            String currentName = parser.currentName();
            token = parser.nextToken();
            if (FIELD_PIT_ID.equals(currentName)) {
                Assert.isTrue(token.isValue(), "invalid response; received non-value point in time field");
                pitId = parser.text();
                token = parser.nextToken();
            } else if (FIELD_AGGREGATIONS.equals(currentName)) {
                result = parseAggregations(parser, copy);
                token = parser.currentToken();
            } else {
                // Skip the field
                if (token.isValue()) {
                    token = parser.nextToken();
                } else {
                    parser.skipChildren();
                    token = parser.nextToken();
                }
            }
        }
        parser.nextToken(); // Eliminate END_OBJECT (Should be at EOF)

        return new CompositeAgg<>(pitId, result);
    }

    private CompositeAggResult<T> parseAggregations(Parser parser, BytesArray copy) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; missing composite aggregate object start");

        // Locate our target aggregation. Non-empty aggregations field is required.
        Token token = parser.nextToken();
        Assert.isTrue(token == Token.FIELD_NAME, "invalid response; missing any aggregations");
        CompositeAggResult<T> compositeAggResult = null;
        while (token != Token.END_OBJECT) {
            String aggregationName = parser.currentName();
            parser.nextToken(); // START_OBJECT
            if (aggregationName.equals(targetAggregationName)) {
                compositeAggResult = parseCompositeAggregation(parser, copy);
                token = parser.currentToken();
            } else {
                // Skip the entire aggregation since it's not the one we're looking for (sanity)
                parser.skipChildren();
                token = parser.nextToken(); // Eliminate END_OBJECT
            }
        }
        parser.nextToken(); // Eliminate END_OBJECT
        Assert.notNull(compositeAggResult, "invalid response; missing composite aggregation");

        return compositeAggResult;
    }

    private CompositeAggResult<T> parseCompositeAggregation(Parser parser, BytesArray copy) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; missing composite aggregate object start");

        Token token = parser.nextToken();

        BytesArray afterKey = null;
        List<T> rows = null;
        while (token != Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (fieldName.equals(FIELD_AFTER_KEY)) {
                afterKey = parseAfterKey(parser, copy);
                token = parser.currentToken();
            } else if (fieldName.equals(FIELD_BUCKETS)) {
                rows = parseBuckets(parser);
                token = parser.currentToken();
            }
        }
        parser.nextToken(); // Eliminate END_OBJECT

        Assert.notNull(afterKey, "invalid response; missing composite aggregate after key");
        Assert.notNull(rows, "invalid response; missing composite aggregate buckets");

        return new CompositeAggResult<>(afterKey, rows, rows.isEmpty());
    }

    private BytesArray parseAfterKey(Parser parser, BytesArray copy) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; missing composite agg after key object start");

        int afterKeyStart = parser.tokenCharOffset();
        parser.skipChildren();
        int afterKeyEnd = parser.tokenCharOffset();
        parser.nextToken(); // Eliminate END_OBJECT
        return new BytesArray(copy.bytes(), afterKeyStart, afterKeyEnd - afterKeyStart);
    }

    private List<T> parseBuckets(Parser parser) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.START_ARRAY, "invalid response; missing composite agg bucket array start");

        List<T> buckets = new ArrayList<>();

        Token token = parser.nextToken();
        while (token != Token.END_ARRAY) {
            T bucketResult = parseBucket(parser);
            buckets.add(bucketResult);
            token = parser.currentToken();
        }
        parser.nextToken(); // Eliminate END_ARRAY

        return buckets;
    }

    private T parseBucket(Parser parser) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; missing bucket object start");

        if (parsingCallback != null) {
            parsingCallback.beginDoc();
            parsingCallback.beginSource();
        }

        Object record = reader.createMap();
        boolean keyFieldsRetrieved = false;

        Token token = parser.nextToken();
        while (token != Token.END_OBJECT) {
            String currentName = parser.currentName();
            parser.nextToken();
            if (FIELD_KEY.equals(currentName)) {
                // The composite key fields
                parseBucketKeyFields(parser, record);
                keyFieldsRetrieved = true;
            } else if (FIELD_DOC_COUNT.equals(currentName)) {
                // The number of docs under this composite key
                parseBucketDocCount(parser, record);
            } else {
                // A sub aggregate under this bucket
                parseBucketSubaggregation(currentName, parser, record);
            }
            token = parser.currentToken();
        }
        parser.nextToken(); // Eliminate END_OBJECT

        // TODO: We should probably verify that we got all the fields that we came here for, otherwise if we're
        //  missing any they'll just not be there. Needs to be considered against the possibility of fields not
        //  being present validly as well.
        Assert.isTrue(keyFieldsRetrieved, "invalid response; missing composite agg bucket key field");

        if (parsingCallback != null) {
            parsingCallback.endSource();
            parsingCallback.endDoc();
        }

        return (T) record; // TODO Can we generic this?
    }

    // TODO: This method is filled with weird value parsing specific stuff. This should probably be refactored so that
    //  things like parsing callbacks are contained and manipulated in one place.
    private void parseBucketKeyFields(Parser parser, Object record) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; missing composite agg bucket key field object start");

        Token token = parser.nextToken();
        while (token != Token.END_OBJECT) {
            String currentName = parser.currentName();

            if (shouldSkipKey(currentName)) {
                // This field is not needed in the final result set.
                Token ignored = parser.nextToken();
                if (ignored.isValue()) {
                    // consume and move along
                    token = parser.nextToken();
                } else {
                    // Shouldn't happen but just in case
                    parser.skipChildren(); // Skip to end token
                    token = parser.nextToken(); // Advance off of end token
                }
            } else {
                reader.beginField(currentName);

                // Read field name and determine mapping type
                Object fieldName = reader.readValue(parser, currentName, FieldType.STRING);
                FieldType esType = mapping(currentName, parser);

                // Validate value token
                token = parser.nextToken();
                Assert.isTrue(token.isValue(), "invalid response; composite agg bucket key field [" + currentName + "] is non value type [" +
                        token + "]");

                // Read value
                try {
                    reader.addToMap(record, fieldName, parseValue(parser, esType));
                } catch (Exception ex) {
                    throw new EsHadoopParsingException(
                            String.format(Locale.ROOT, "Cannot parse value [%s] for field [%s]", parser.text(), fieldName), ex);
                }

                reader.endField(currentName);

                // parseValue advances to next token already
                token = parser.currentToken();
            }
        }
        parser.nextToken(); // eliminate END_OBJECT before return
    }

    private boolean shouldSkipKey(String absoluteFieldName) {
        // TODO: Check if we actually want to read this key field
        //  The field may be important for group by, but we might have a projection
        //  that doesn't include it in the final results.
        return false;
    }

    private void parseBucketDocCount(Parser parser, Object record) {
        Assert.notNull(parser);
        Assert.isTrue(parser.currentToken() == Token.VALUE_NUMBER, "invalid response; composite doc count malformed");

        if (countStarAgg.isPresent()) {
            AggInfo countStarAgg = this.countStarAgg.get();
            reader.beginField(countStarAgg.getFieldName());
            reader.addToMap(record, reader.wrapString(countStarAgg.getFieldName()), parseValue(parser, countStarAgg.getFieldType()));
            reader.endField(countStarAgg.getFieldName());
            // parseValue advances to next token
        } else {
            parser.nextToken(); // Advance to next field
        }
    }

    private void parseBucketSubaggregation(String aggName, Parser parser, Object record) {
        Assert.notNull(aggName);
        Assert.notNull(parser);
        Assert.notNull(record);
        Assert.isTrue(parser.currentToken() == Token.START_OBJECT, "invalid response; missing sub-aggregation [" + aggName + "] body");

        AggInfo aggInfo = targetAggs.get(aggName);
        if (aggInfo == null) {
            // We don't need this aggregate, skip
            parser.skipChildren();
            parser.nextToken();
        } else {
            boolean aggregationResultConsumed = false;
            Token token = parser.nextToken();
            while (token != Token.END_OBJECT) {
                if (aggregationResultConsumed) {
                    // Early out - If there are still fields in the agg result but we've already got our value we need,
                    // skip the block and break the loop.
                    ParsingUtils.skipCurrentBlock(parser);
                    break;
                }
                String currentName = parser.currentName();
                token = parser.nextToken();
                if (FIELD_VALUE.equals(currentName)) {
                    reader.beginField(aggInfo.getFieldName());
                    reader.addToMap(record, reader.wrapString(aggInfo.getFieldKey()), parseValue(parser, aggInfo.getFieldType()));
                    reader.endField(aggInfo.getFieldName());
                    aggregationResultConsumed = true;
                    token = parser.currentToken();
                } else {
                    // A field on the sub agg that we don't care about. Skip it instead of reading (compatibility).
                    if (token.isValue()) {
                        token = parser.nextToken();
                    } else {
                        parser.skipChildren();
                        token = parser.nextToken();
                    }
                }
            }
            parser.nextToken(); // Eliminate END_OBJECT
            Assert.isTrue(aggregationResultConsumed, "invalid response; missing sub-aggregation [" + aggName + "] value field");
        }
    }

    /**
     * TODO: Directly from ScrollReader
     */
    private FieldType mapping(String fieldMapping, Parser parser) {
        FieldType esType = esMapping.get(fieldMapping);

        if (esType != null) {
            return esType;
        }

        // fall back to JSON
        Token currentToken = parser.currentToken();
        if (!currentToken.isValue()) {
            // nested type
            return FieldType.OBJECT;
        }

        switch (currentToken) {
            case VALUE_NULL:
                esType = FieldType.NULL;
                break;
            case VALUE_BOOLEAN:
                esType = FieldType.BOOLEAN;
                break;
            case VALUE_STRING:
                esType = FieldType.STRING;
                break;
            case VALUE_NUMBER:
                Parser.NumberType numberType = parser.numberType();
                switch (numberType) {
                    case INT:
                        esType = FieldType.INTEGER;
                        break;
                    case LONG:
                        esType = FieldType.LONG;
                        break;
                    case FLOAT:
                        esType = FieldType.FLOAT;
                        break;
                    case DOUBLE:
                        esType = FieldType.DOUBLE;
                        break;
                    case BIG_DECIMAL:
                        throw new UnsupportedOperationException();
                    case BIG_INTEGER:
                        throw new UnsupportedOperationException();
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return esType;
    }

    /**
     * TODO: Copied directly from ScrollReader
     */
    private Object parseValue(Parser parser, FieldType esType) {
        Object obj;
        // special case of handing null (as text() will return "null")
        if (parser.currentToken() == Token.VALUE_NULL) {
            obj = null;
        }
        else {
            obj = reader.readValue(parser, parser.text(), esType);
        }
        parser.nextToken();
        return obj;
    }
}
