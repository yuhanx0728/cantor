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
     *
     * If alter table query contains both metadata."a" and dimensions."a", it will throw an exception for illegal
     * concurrent operations. That is why alter table queries for metadata and dimensions are separated.
     *
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
            for (String key : dKeys) {
                dimensionsColNameJoiner.add("\"d_" + key + "\"" + " DOUBLE");
            }
            for (String key : mKeys) {
                metadataColNameJoiner.add("\"m_" + key + "\"" + " VARCHAR");
            }
            alterTableDimensionsBuilder.append(dimensionsColNameJoiner.toString());
            alterTableMetadataBuilder.append(metadataColNameJoiner.toString());
            executeUpdate(connection, alterTableDimensionsBuilder.toString());
            executeUpdate(connection, alterTableMetadataBuilder.toString());

            for (Event e : batch) {
                colNameJoiner.add("timestampMillis").add("namespace").add("payload");
                upsertParameters.add(e.getTimestampMillis());
                upsertParameters.add(namespace);
                upsertParameters.add(e.getPayload());
                Map<String, Double> dimensions = e.getDimensions();
                paramJoiner.add("?").add("?").add("?");
                for (String key : dimensions.keySet()) {
                    colNameJoiner.add("\"d_" + key + "\"");
                    upsertParameters.add(dimensions.get(key));
                    paramJoiner.add("?");
                }
                Map<String, String> metadata = e.getMetadata();
                for (String key: metadata.keySet()) {
                    colNameJoiner.add("\"m_" + key + "\"");
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
        StringBuilder query = new StringBuilder("select * from cantor_events where ");
        StringJoiner queryJoiner = new StringJoiner(" and ");
        queryJoiner.add("timestampMillis between " + startTimestampMillis + " and " + endTimestampMillis);
        queryJoiner.add("namespace = \'" + namespace + "\'");
        getDimensionQueries(dimensionsQuery, queryJoiner); // update queryJoiner with dimensions queries
        getMetadataQueries(metadataQuery, queryJoiner); // update queryJoiner with metadata queries
        query.append(queryJoiner.toString());
        if (ascending) {
            query.append(" order by timestampMillis asc");
        } else {
            query.append(" order by timestampMillis desc");
        }
        if (limit > 0) {
            query.append(" limit ").append(limit);
        }
        System.out.println(query.toString());
        try {
            Connection con = getConnection();
            PreparedStatement statement = con.prepareStatement(query.toString());
            rset = statement.executeQuery();
            while (rset.next()) {
                Map<String, String> metadata = new HashMap<>();
                Map<String, Double> dimensions = new HashMap<>();
                for (int i = 1; i <= rset.getMetaData().getColumnCount(); i ++) {
                    final String columnName = rset.getMetaData().getColumnName(i);
                    if (columnName.startsWith("m_")) { //TODO: what if empty metadata?
                        metadata.put(columnName.substring(2), rset.getString(i));
                    } else if (columnName.startsWith("d_")) { //TODO: what if empty dimensions?
                        dimensions.put(columnName.substring(2), rset.getDouble(i));
                    }
                }
                Event e = new Event(rset.getLong("timestampMillis"), metadata, dimensions,
                            (includePayloads) ? rset.getBytes("payload") : null);
                events.add(e);
            }
            statement.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return events;
    }

    private void getDimensionQueries(Map<String, String> dimensionsQuery, StringJoiner queryJoiner) {
        if (dimensionsQuery == null) {
            return;
        }
        for (Map.Entry<String, String> query : dimensionsQuery.entrySet()) {
            String column = query.getKey();
            String condition = query.getValue();
            queryJoiner.add("\"d_" + column + "\"" + condition);
        }
    }

    private void getMetadataQueries(Map<String, String> metadataQuery, StringJoiner queryJoiner) {
        if (metadataQuery == null) {
            return;
        }
        for (Map.Entry<String, String> query : metadataQuery.entrySet()) {
            String column = query.getKey();
            String condition = query.getValue();
            if (condition.startsWith("~")) {
                queryJoiner.add("\"m_" + column + "\" like \'" + condition.substring(1).replaceAll("\\.\\*", "%") + "\'");
            } else { // exact match
                queryJoiner.add("\"m_" + column + "\" = \'" + ((condition.startsWith("=")) ? condition.substring(1): condition) + "\'");
            }
        }
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
