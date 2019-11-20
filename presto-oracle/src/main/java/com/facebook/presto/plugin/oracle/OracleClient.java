/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.*;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.sql.ResultSetMetaData.columnNullable;

/**
 * OracleClient is where the actual connection to Oracle is built
 * DriverConnectionFactory is what does the work of actually connecting and applying JdbcOptions
 */
public class OracleClient
        extends BaseJdbcClient
{
    private static final Logger LOG = Logger.get(OracleClient.class);
    private static final String META_DB_NAME_FIELD = "TABLE_SCHEM";
    private static final String QUERY_SCHEMA_SYNS = String.format("SELECT distinct(OWNER) AS %s FROM SYS.ALL_SYNONYMS", META_DB_NAME_FIELD);
    private static final String QUERY_TABLE_SYNS = "SELECT TABLE_OWNER, TABLE_NAME FROM SYS.ALL_SYNONYMS WHERE OWNER = ? AND SYNONYM_NAME = ?;";
    private OracleConfig oracleConfig;

    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OracleConfig oracleConfig)
            throws SQLException
    {
        super(connectorId, config, Character.toString('"'), new DriverConnectionFactory(new OracleDriver(), config));
        this.oracleConfig = oracleConfig;
    }

    @Override
    /**
     * SELECT distinct(owner) AS DATABASE_SCHEM FROM SYS.ALL_SYNONYMS;
     * SCHEMA synonyms must be included in the Schema List, ALL_SYNONYMS are any synonym visible by the current user.
     */
    protected Collection<String> listSchemas(Connection connection)
    {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            while (resultSet.next()) {
                // Schema Names are in "TABLE_SCHEM" for Oracle
                String schemaName = resultSet.getString(META_DB_NAME_FIELD);
                if(schemaName == null) {
                    LOG.error("connection.getMetaData().getSchemas() returned null schema name");
                    continue;
                }
                // skip internal schemas
                if (schemaName.equalsIgnoreCase("information_schema")) {
                    continue;
                }

                schemaNames.add(schemaName);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        // Merge schema synonyms with all schema names.
        if(oracleConfig.isSynonymsEnabled()) {
            try {
                schemaNames.addAll(listSchemaSynonyms(connection));
            } catch (PrestoException ex2) {
                LOG.error(ex2);
            }
        }
        return schemaNames.build();
    }

    private Collection<String> listSchemaSynonyms(Connection connection) {
        ImmutableSet.Builder<String> schemaSynonyms = ImmutableSet.builder();
        try {
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(QUERY_SCHEMA_SYNS);
            while (resultSet.next()) {
                String schemaSynonym = resultSet.getString(META_DB_NAME_FIELD);
                schemaSynonyms.add(schemaSynonym);
            }
        } catch (SQLException e) {
            throw new PrestoException(
                    JDBC_ERROR, String.format("Failed retrieving schema synonyms, query was: %s", QUERY_SCHEMA_SYNS));
        }

        return schemaSynonyms.build();
    }

    @Override
    /**
     * Retrieve information about tables/views using the JDBC Drivers DatabaseMetaData api,
     * Include "SYNONYM" - functionality specific to Oracle
     */
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // Exactly like the parent class, except we include "SYNONYM" - specific to Oracle
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "SYNONYM", "GLOBAL TEMPORARY", "LOCAL TEMPORARY"});
    }

    /*
    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());

            List<JdbcTableHandle> tableHandles = new ArrayList<>();
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,                                  // schemaTableName
                            resultSet.getString("TABLE_CAT"),    // catalog
                            resultSet.getString(META_DB_NAME_FIELD),         // schema
                            resultSet.getString("TABLE_NAME"))); // table name
                }

                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
     */

    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString(META_DB_NAME_FIELD);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            if(oracleConfig.isSynonymsEnabled()) {
                ((oracle.jdbc.driver.OracleConnection) connection).setIncludeSynonyms(true);
            }
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }

    /**
     * Return an anonymous method that acts as a type mapper for the given column type.
     * Each method is called a ReadMapping, which reads data in and converts the JDBC type to a supported Presto Type
     * For more details see OracleReadMappings.java
     *
     * See: https://github.com/prestodb/presto/blob/3060c65a1812c6c8b0c2ab725b0184dbad67f0ed/presto-base-jdbc/src/main/java/com/facebook/presto/plugin/jdbc/StandardReadMappings.java
     *
     * JdbcRecordCursor is what calls this method
     *
     * @param session
     * @param typeHandle
     * @return
     */
    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String error = "";
        OracleJdbcTypeHandle orcTypeHandle = new OracleJdbcTypeHandle(typeHandle);

        // -- Handle JDBC to Presto Type Mappings -------------------------------------------------
        Optional<ReadMapping> readType = Optional.empty();
        switch (typeHandle.getJdbcType()) {
            case Types.DATE:
                // Oracle DATE values may store hours, minutes, and seconds, so they are mapped to TIMESTAMP in Presto.
                // treat oracle DATE as TIMESTAMP (java.sql.Timestamp)
                readType = Optional.of(timestampReadMapping());
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                LOG.info("PRESTO-TYPE_HANDLE %s(%d, %d)", JDBCType.valueOf(typeHandle.getJdbcType()), typeHandle.getColumnSize(), typeHandle.getDecimalDigits());
                LOG.info("ORACLE-TYPE_HANDLE %s", orcTypeHandle.getDescription());
                LOG.info("ORACLE-SCALE-UNDEFINED %s", orcTypeHandle.isScaleUndefined());
                try {
                    OracleNumberHandling numberHandling = new OracleNumberHandling(orcTypeHandle, this.oracleConfig);
                    readType = Optional.of(numberHandling.getReadMapping());
                    Type prestoType = readType.get().getType();
                    LOG.info("ORACLE-READ_TYPE %s", prestoType);

                } catch (IgnoreFieldException ex) {
                    return Optional.empty(); // skip field
                } catch (PrestoException ex) {
                    error  = ex.toString();
                }
                LOG.info("------------------------------------------------");
                break;
            default:
                readType = super.toPrestoType(session, typeHandle);
        }

        if(!readType.isPresent()) {
            String msg = String.format("unsupported type %s - %s", orcTypeHandle.getDescription(), error);
            switch(oracleConfig.getUnsupportedTypeStrategy()) {
                case CONVERT_TO_VARCHAR:
                    readType = Optional.of(StandardReadMappings.varcharReadMapping(createUnboundedVarcharType()));
                    break;
                case IGNORE:
                    LOG.info(msg + " - 'unsupported-type.handling-strategy' = IGNORE");
                case FAIL:
                    throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                            msg + " - 'unsupported-type.handling-strategy' = FAIL");

            }
        }
        return readType;
    }
}
