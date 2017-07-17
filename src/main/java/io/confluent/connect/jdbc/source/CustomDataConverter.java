package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class CustomDataConverter {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    public static Schema convertSchema(String tableName, ResultSetMetaData metadata, boolean mapNumerics, Set<String> outputFields)
            throws SQLException {
        // TODO: Detect changes to metadata, which will require schema updates
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName);
        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            DataConverter.addFieldSchema(metadata, col, builder, mapNumerics);
        }
        if (outputFields != null && !outputFields.isEmpty()) {
            for (String outputField : outputFields) {
                Schema nested = SchemaBuilder.array(Schema.INT64_SCHEMA).optional().build();
                builder.field(outputField, nested);
            }
            log.debug("Built success nested schema");
        }
        return builder.build();
    }

    public static Struct convertRecord(Schema schema, ResultSet resultSet, boolean mapNumerics, Map<String, String[]> convertFields)
            throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        Struct struct = new Struct(schema);
        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            try {
                DataConverter.convertFieldValue(resultSet, col, metadata.getColumnType(col), struct,
                        metadata.getColumnLabel(col), mapNumerics);
            } catch (IOException e) {
                log.warn("Ignoring record because processing failed:", e);
            } catch (SQLException e) {
                log.warn("Ignoring record due to SQL error:", e);
            }
        }
        if (convertFields != null && !convertFields.isEmpty()) {
            for (Map.Entry<String, String[]> entry : convertFields.entrySet()) {
                convertNestedField(struct, entry.getValue(), entry.getKey());
            }
        }
        return struct;
    }

    private static void convertNestedField(Struct struct, String[] inputFieldNames, String outputField) {
        HashSet<Long> longSet = new HashSet<>();
        if (inputFieldNames == null || inputFieldNames.length == 0) {
            return;
        }
        List<String> values = new ArrayList<>();
        for (String name : inputFieldNames) {
            try {
                Object field = struct.get(name);
                values.add(String.valueOf(field));
            } catch (Exception e) {
                log.warn("Ignoring record because get nested input field fail:", e);
            }
        }
        if (values.isEmpty()) {
            return;
        }
        String[] longStrings = StringUtils.join(values, ",").split(",");
        for (String longString : longStrings) {
            try {
                long longValue = Long.parseLong(longString);
                longSet.add(longValue);
            } catch (NumberFormatException e) {
                log.warn("Ignoring record because convert nested input field to long fail:", e);
            }
        }
        ArrayList longList = new ArrayList(longSet);
        struct.put(outputField, longList);
    }
}
