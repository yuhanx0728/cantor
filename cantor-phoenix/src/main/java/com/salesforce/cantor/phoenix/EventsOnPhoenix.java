package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class EventsOnPhoenix extends AbstractBaseEventsOnJdbc implements Events {

    public EventsOnPhoenix(final DataSource dataSource) throws IOException {
        super(dataSource);
        String createTableSql = "create table if not exists cantor_events (" +
                                    "id BIGINT not null, " +
                                    "timestampMillis BIGINT not null, " +
                                    "namespace VARCHAR, " +
                                    "dimensions.iid INTEGER, " +
                                    "metadata.iid INTEGER, " +
                                    "payload VARBINARY " +
                                    "CONSTRAINT pk PRIMARY KEY (id, timestampMillis))";
        String createSequenceSql = "create sequence if not exists cantor_events_id";
        executeUpdate(createTableSql);
        executeUpdate(createSequenceSql);
    }

    /**
     * column names under metadata and dimensions column families are case sensitive and thus need to have quotes around
     * them in sql queries, e.g. select metadata."host" from cantor_events. It is because said names can contain special
     * characters like "-" that would not be parsed as names without the quotes.
     * @param namespace the namespace identifier
     * @param batch batch of events
     * @throws IOException
     */
    @Override
    public void store(String namespace, Collection<Event> batch) throws IOException {
        StringBuilder alterTableMetadataBuilder = new StringBuilder("alter table cantor_events add if not exists ");
        StringBuilder alterTableDimensionsBuilder = new StringBuilder("alter table cantor_events add if not exists ");
        StringJoiner metadataColNameJoiner = new StringJoiner(", ");
        StringJoiner dimensionsColNameJoiner = new StringJoiner(", ");
        StringBuilder upsertSqlPrefix = new StringBuilder("upsert into cantor_events (id, ");
        StringBuilder upsertSqlInfix = new StringBuilder(") values (next value for cantor_events_id, ");
        StringJoiner colNameJoiner = new StringJoiner(", ");
        StringJoiner paramJoiner = new StringJoiner(", ");
        Connection connection = null;
        try {
            connection = openTransaction(getConnection());
            final List<Object> upsertParameters = new ArrayList<>();
            final Set<String> mKeys = new HashSet<>();
            final Set<String> dKeys = new HashSet<>();
            for (Event e : batch) {
                mKeys.addAll(e.getMetadata().keySet());
                dKeys.addAll(e.getDimensions().keySet());
            }
            for (String key : mKeys) {
                metadataColNameJoiner.add("metadata.\"" + key + "\"" + " VARCHAR");
            }
            alterTableMetadataBuilder.append(metadataColNameJoiner.toString());
            executeUpdate(connection, alterTableMetadataBuilder.toString());

            for (String key : dKeys) {
                dimensionsColNameJoiner.add("dimensions.\"" + key + "\"" + " DOUBLE");
            }
            alterTableDimensionsBuilder.append(dimensionsColNameJoiner.toString());
            executeUpdate(connection, alterTableDimensionsBuilder.toString());

            for (Event e : batch) {
                colNameJoiner.add("timestampMillis").add("namespace").add("payload");
                upsertParameters.add(e.getTimestampMillis());
                upsertParameters.add(namespace);
                upsertParameters.add(e.getPayload());
                Map<String, Double> dimensions = e.getDimensions();
                paramJoiner.add("?").add("?").add("?");
                for (String key : dimensions.keySet()) {
                    colNameJoiner.add("dimensions.\"" + key + "\"");
                    upsertParameters.add(dimensions.get(key));
                    paramJoiner.add("?");
                }
                Map<String, String> metadata = e.getMetadata();
                for (String key: metadata.keySet()) {
                    colNameJoiner.add("metadata.\"" + key + "\"");
                    upsertParameters.add(metadata.get(key));
                    paramJoiner.add("?");
                }
                executeUpdate(connection, upsertSqlPrefix.append(colNameJoiner.toString()).append(upsertSqlInfix)
                                .append(paramJoiner.toString()).append(")").toString(), upsertParameters.toArray());
            }
        } finally {
            closeConnection(connection);
        }
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
        Map<String, String> metadata = new HashMap<>();
        String[] lines = metadataString.split("\n");
        if (lines.length == 2) {
            String[] header = lines[0].split(";;");
            String[] values = lines[1].split(";;");
            for (int i = 0; i < header.length; i ++) {
                metadata.put(header[i], values[i].substring(1, values[i].length() - 1));
            }
        }
        return metadata;
    }

    private Map<String, Double> deserializeDimensions(String dimensionsString) {
        Map<String, Double> dimensions = new HashMap<>();
        String[] lines = dimensionsString.split("\n");
        if (lines.length == 2) {
            String[] header = lines[0].split(";;");
            String[] values = lines[1].split(";;");
            for (int i = 0; i < header.length; i ++) {
                dimensions.put(header[i], Double.parseDouble(values[i]));
            }
        }
        return dimensions;
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
