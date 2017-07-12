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
import java.util.ArrayList;
import java.util.List;

public class CustomDataConverter {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    public static Schema convertSchema(String tableName, ResultSetMetaData metadata, boolean mapNumerics, String outputField)
            throws SQLException {
        // TODO: Detect changes to metadata, which will require schema updates
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName);
        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            DataConverter.addFieldSchema(metadata, col, builder, mapNumerics);
        }
        if (outputField != null) {
            Schema nested = SchemaBuilder.array(Schema.INT64_SCHEMA).optional().build();
            builder.field(outputField, nested);
        }
        return builder.build();
    }

    public static Struct convertRecord(Schema schema, ResultSet resultSet, boolean mapNumerics, List<String> inputFieldNames, String outputField)
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
        if (outputField != null) {
            convertNestedField(struct, inputFieldNames, outputField);
        }
        return struct;
    }

    private static void convertNestedField(Struct struct, List<String> inputFieldNames, String outputField) {
        ArrayList<Long> longList = new ArrayList<>();
        if (inputFieldNames == null || inputFieldNames.isEmpty()) {
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
                longList.add(longValue);
            } catch (NumberFormatException e) {
                log.warn("Ignoring record because convert nested input field to long fail:", e);
            }
        }
        struct.put(outputField, longList);
    }
}
