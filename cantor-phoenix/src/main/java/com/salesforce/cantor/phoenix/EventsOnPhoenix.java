package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class EventsOnPhoenix extends AbstractBaseEventsOnJdbc implements Events {

    public EventsOnPhoenix(final DataSource dataSource) throws IOException {
        super(dataSource);
        String createTableSql = "CREATE TABLE IF NOT EXISTS CANTOR_EVENTS (timestampMillis BIGINT NOT NULL, " +
                "namespace VARCHAR, dimensions VARCHAR, metadata VARCHAR, payload VARBINARY, id BIGINT NOT NULL " +
                "CONSTRAINT pk PRIMARY KEY (timestampMillis, id))";
        String createSequenceSql = "CREATE SEQUENCE IF NOT EXISTS CANTOR_EVENTS_ID";
        executeUpdate(createTableSql);
        executeUpdate(createSequenceSql);
    }

    @Override
    public void store(String namespace, Collection<Event> batch) throws IOException {
        String upsertSql = "UPSERT INTO CANTOR_EVENTS VALUES (?, ?, ?, ?, ?, NEXT VALUE FOR CANTOR_EVENTS_ID)";
        Connection connection = null;
        try {
            connection = openTransaction(getConnection());
            final List<Object[]> parameters = new ArrayList<>();
            for (Event e : batch) {
                parameters.add(new Object[]{e.getTimestampMillis(), namespace, serializeDimensions(e.getDimensions()),
                        serializeMetadata(e.getMetadata()), e.getPayload()});
            }
            executeBatchUpdate(connection, upsertSql, parameters);
        } finally {
            closeConnection(connection);
        }
    }

    private String serializeDimensions(Map<String, Double> dimensions) {
        return new Gson().toJson(dimensions, new TypeToken<Map<String, Double>>(){}.getType());
    }

    private String serializeMetadata(Map<String, String> metadata) {
        return new Gson().toJson(metadata, new TypeToken<Map<String, String>>(){}.getType());
    }

    @Override
    public List<Event> get(String namespace, long startTimestampMillis, long endTimestampMillis,
                           Map<String, String> metadataQuery, Map<String, String> dimensionsQuery,
                           boolean includePayloads, boolean ascending, int limit) throws IOException {
        List<Event> events = new ArrayList<>();
        ResultSet rset;
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM CANTOR_EVENTS WHERE timestampMillis >= ").append(startTimestampMillis)
             .append(" AND timestampMillis < ").append(endTimestampMillis)
             .append(" AND namespace = '").append(namespace).append("'");
        if (dimensionsQuery != null) {
            for (String key : dimensionsQuery.keySet()) {
                query.append(" AND INSTR(dimensions, '").append(key).append("') > 0");
            }
        }
        if (metadataQuery != null) {
            for (String key : metadataQuery.keySet()) {
                query.append(" AND INSTR(metadata, '").append(key).append("') > 0");
            }
        }
        if (ascending) {
            query.append(" ORDER BY timestampMillis ASC");
        } else {
            query.append(" ORDER BY timestampMillis DESC");
        }
        if (limit > 0) {
            query.append(" LIMIT ").append(limit);
        }
        try {
            Connection con = getConnection();
            PreparedStatement statement = con.prepareStatement(query.toString());
            rset = statement.executeQuery();
            while (rset.next()) {
                Map<String, String> metadata = deserializeMetadata(rset.getString("metadata"));
                Map<String, Double> dimensions = deserializeDimensions(rset.getString("dimensions"));
                if (dimensionsMatches(dimensionsQuery, dimensions) && metadataMatches(metadataQuery, metadata)) {
                    Event e = new Event(rset.getLong("timestampMillis"), metadata, dimensions,
                            (includePayloads) ? rset.getBytes("payload") : null);
                    events.add(e);
                }
            }
            statement.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return events;
    }

    private boolean metadataMatches(Map<String, String> query, Map<String, String> metadata) {
        if (query == null) {
            return true;
        }
        for (Map.Entry<String, String> entry : query.entrySet()) {
            String md = entry.getKey();
            String exp = entry.getValue();
            if (exp.startsWith("~")) {
                String newExp = exp.substring(1).replace("*", ".*");
                if (!metadata.get(md).matches(newExp)) {
                    return false;
                }
            } else {
                String newExp = (exp.startsWith("=")) ? exp.substring(1) : exp;
                if (metadata.get(md) != newExp) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean dimensionsMatches(Map<String, String> query, Map<String, Double> dimensions) {
        if (query == null) {
            return true;
        }
        for (Map.Entry<String, String> entry : query.entrySet()) {
            String dim = entry.getKey();
            String exp = entry.getValue();
            if (exp.startsWith(">=") && !(dimensions.get(dim) >= Double.parseDouble(exp.substring(2)))) {
                return false;
            } else if (exp.startsWith("<=") && !(dimensions.get(dim) <= Double.parseDouble(exp.substring(2)))) {
                return false;
            } else if (exp.startsWith(">") && !(dimensions.get(dim) > Double.parseDouble(exp.substring(1)))) {
                return false;
            } else if (exp.startsWith("<") && !(dimensions.get(dim) < Double.parseDouble(exp.substring(1)))) {
                return false;
            } else if (exp.startsWith("=") && dimensions.get(dim) != Double.parseDouble(exp.substring(1))) {
                return false;
            } else if (exp.contains("..")) {
                double start = Double.parseDouble(exp.substring(0, exp.indexOf("..")));
                double end = Double.parseDouble(exp.substring(exp.indexOf("..")+2));
                if (dimensions.get(dim) <= start || dimensions.get(dim) >= end) {
                    return false;
                }
            }
        }
        return true;
    }

    private Map<String, String> deserializeMetadata(String metadataString) {
        return new Gson().fromJson(metadataString, new TypeToken<Map<String, String>>(){}.getType());
    }

    private Map<String, Double> deserializeDimensions(String dimensionsString) {
        return new Gson().fromJson(dimensionsString, new TypeToken<Map<String, Double>>(){}.getType());
    }

    @Override
    public int delete(String namespace, long startTimestampMillis, long endTimestampMillis,
                      Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) throws IOException {
        return 0;
    }

    @Override
    public Map<Long, Double> aggregate(String namespace, String dimension, long startTimestampMillis,
                                       long endTimestampMillis, Map<String, String> metadataQuery,
                                       Map<String, String> dimensionsQuery, int aggregateIntervalMillis,
                                       AggregationFunction aggregationFunction) throws IOException {
        return null;
    }

    @Override
    public Set<String> metadata(String namespace, String metadataKey, long startTimestampMillis,
                                long endTimestampMillis, Map<String, String> metadataQuery,
                                Map<String, String> dimensionsQuery) throws IOException {
        return null;
    }

    @Override
    public void expire(String namespace, long endTimestampMillis) throws IOException {

    }

    @Override
    protected String getCreateChunkLookupTableSql(String namespace) {
        return null;
    }

    @Override
    protected String getCreateChunkTableSql(String chunkTableName, String namespace, Map<String, String> metadata, Map<String, Double> dimensions) {
        return null;
    }

    @Override
    protected String getRegexQuery(String column) {
        return null;
    }

    @Override
    protected String getNotRegexQuery(String column) {
        return null;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return null;
    }

    @Override
    public void create(String namespace) throws IOException {
    }

    @Override
    public void drop(String namespace) throws IOException {
    }
}
