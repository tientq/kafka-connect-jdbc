package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CustomTimestampIncrementingTableQuerier extends TimestampIncrementingTableQuerier {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
    private final String partitionKeyColumn;
    private String inputFields;
    private String outputField;
    private List<String> inputFiledNames = new ArrayList<>();

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
        inputFields = config.getString(JdbcSourceTaskConfig.INPUT_FIELDS_CONF);
        outputField = config.getString(JdbcSourceTaskConfig.OUTPUT_FIELD_CONF);
        removeOutputFieldIfInputInvalid();
        updateInputFieldNames();
    }

    private void removeOutputFieldIfInputInvalid() {
        if (inputFields == null || "".equals(inputFields.trim()) || outputField == null || "".equals(outputField.trim())) {
            inputFields = null;
            outputField = null;
            log.warn("Invalid input fields or output field in readable convert");
            return;
        }
        inputFields = inputFields.trim();
        outputField = outputField.trim();
    }

    private void updateInputFieldNames() {
        if (inputFields == null) {
            return;
        }
        String[] names = inputFields.split(",");
        for (String name : names) {
            if (name == null || "".equals(name.trim())) {
                continue;
            }
            inputFiledNames.add(name.trim());
        }
        if (inputFiledNames.isEmpty()) {
            inputFields = null;
            outputField = null;
            log.warn("Invalid input fields or output field in readable convert");
        }
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        final Struct record = CustomDataConverter.convertRecord(schema, resultSet, mapNumerics, inputFiledNames, outputField);
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
            schema = CustomDataConverter.convertSchema(name, resultSet.getMetaData(), mapNumerics, outputField);
        }
    }
}
