package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomTimestampIncrementingTableQuerier extends TimestampIncrementingTableQuerier {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
    private final String partitionKeyColumn;
    private final String convertPatterns;
    private Map<String, String[]> convertFields = new HashMap<>();

    public CustomTimestampIncrementingTableQuerier(JdbcSourceTaskConfig config, QueryMode queryMode, String tableOrQuery, String timestampColumn, String incrementingColumn, Map<String, Object> offset) {
        super(queryMode,
                tableOrQuery,
                config.getString(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG),
                timestampColumn,
                incrementingColumn,
                offset,
                config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG),
                config.getString(JdbcSourceTaskConfig.SCHEMA_PATTERN_CONFIG),
                config.getBoolean(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG)
        );
        partitionKeyColumn = config.getString(JdbcSourceTaskConfig.PARTITION_KEY_COLUMN_NAME_CONFIG);
        convertPatterns = config.getString(JdbcSourceTaskConfig.CONVERT_FIELDS_CONF);
        buildConvertFields(convertPatterns, tableOrQuery);
    }

    private void buildConvertFields(String convertPatterns, String tableOrQuery) {
        if (convertPatterns == null || "".equals(convertPatterns)) {
            return;
        }
        Pattern tablePattern = Pattern.compile(tableOrQuery + "\\(([a-zA-Z0-9_=+,]+)\\)");
        Pattern fieldPattern = Pattern.compile("([a-zA-Z0-9_]+)=([a-zA-Z0-9_+]+)");
        Matcher tableMatcher = tablePattern.matcher(convertPatterns);
        if (tableMatcher.find()) {
            Matcher fieldMatcher = fieldPattern.matcher(tableMatcher.group(1));
            while (fieldMatcher.find()) {
                String output = fieldMatcher.group(1);
                String input = fieldMatcher.group(2);
                String[] inputFields = input.split("\\+");
                convertFields.put(output, inputFields);
            }
        }
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        final Struct record = CustomDataConverter.convertRecord(schema, resultSet, mapNumerics, convertFields);
        offset = extractOffset(schema, record);
        // TODO: Key?
        final String topic;
        final Map<String, String> partition;
        switch (mode) {
            case TABLE:
                partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
                topic = topicPrefix + name;
                break;
            case QUERY:
                partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                        JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
                topic = topicPrefix;
                break;
            default:
                throw new ConnectException("Unexpected query mode: " + mode);
        }
        if (partitionKeyColumn != null && !partitionKeyColumn.isEmpty()) {
            String key = String.valueOf(record.get(partitionKeyColumn));
            return new SourceRecord(partition, offset.toMap(), topic, Schema.STRING_SCHEMA, key, record.schema(), record);
        }
        return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
    }

    @Override
    public void maybeStartQuery(Connection db) throws SQLException {
        super.maybeStartQuery(db);
        if (resultSet != null) {
            schema = CustomDataConverter.convertSchema(name, resultSet.getMetaData(), mapNumerics, convertFields.keySet());
        }
    }
}
