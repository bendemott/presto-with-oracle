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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Decimals;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import oracle.jdbc.OracleDriver;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.math.RoundingMode;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.integerReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.jdbcTypeToPrestoType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.max;
import static java.lang.Math.min;

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
        try {
            schemaNames.addAll(listSchemaSynonyms(connection));
        } catch (PrestoException ex2) {
            LOG.error(ex2);
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

    /**
     * Retrieves the underlying schema and table-name for a given SCHEMA.TABLE synonym alias.
     * Returns null if no synonym is found.
     *
     * @param connection
     * @param schemaTableName
     * @return
     */
    private SchemaTableName getTableSynonym(Connection connection, SchemaTableName schemaTableName) {
        try {
            // Find a synonym that matches the current schema/table name.
            PreparedStatement stmt = connection.prepareStatement(QUERY_TABLE_SYNS);
            stmt.setString(1, schemaTableName.getSchemaName());
            stmt.setString(2, schemaTableName.getTableName());
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                String schema = rs.getString("TABLE_OWNER");
                String table = rs.getString("TABLE_NAME");
                SchemaTableName toSchemaTable = new SchemaTableName(schema, table);
                return toSchemaTable;
            } else {
                return null;
            }
        } catch (SQLException e) {
            PrestoException ex = new PrestoException(
                    JDBC_ERROR, String.format("Failed retrieving table synonym, query was: %s", QUERY_TABLE_SYNS));
            ex.initCause(e);
            throw ex;
        }
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

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            // TODO needs to be replaced to include synonyms (if enabled)
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

    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString(META_DB_NAME_FIELD);
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
        // The COLUMN_SIZE column specifies the column size for the given column. For numeric data, this is the maximum precision of the column

        // -- Get configuration options from Oracle catalog ---------------------------------------
        RoundingMode roundMode = oracleConfig.getRoundingMode();
        int defaultScale = oracleConfig.getDefaultNumberScale();
        UnsupportedTypeHandling unsupportedMode = oracleConfig.getUnsupportedTypeStrategy();
        boolean undefinedNumberScaleAsInt = oracleConfig.isUndefinedScaleInteger();

        // -- Get Column Field Size  --------------------------------------------------------------
        int columnSize = typeHandle.getColumnSize();

        // -- Handle JDBC to Presto Type Mappings -------------------------------------------------
        Optional<ReadMapping> readType = Optional.empty();
        switch (typeHandle.getJdbcType()) {
            case Types.DATE:
                // Oracle DATE values may store hours, minutes, and seconds, so they are mapped to TIMESTAMP in Presto.
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                ReadMapping readFunc;
                // decimalDigits provides NUMERIC Scale (to the right of the decimal place)
                int decimalDigits = typeHandle.getDecimalDigits();
                int absScale = Math.abs(decimalDigits);
                // columnSize provides NUMERIC Precision (total significant digits)
                int realPrecision = columnSize + max(-decimalDigits, 0);
                int logicalPrecision = min(realPrecision, Decimals.MAX_PRECISION); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                int logicalScale = min(max(decimalDigits, 0), Decimals.MAX_PRECISION);
                String typeName = JDBCType.valueOf(typeHandle.getJdbcType()).getName();

                boolean unsupported = true;
                String reason = null;

                if(decimalDigits == 0 && defaultScale == OracleConfig.UNDEFINED_SCALE && !undefinedNumberScaleAsInt) {
                    reason = "type has no scale (0) and 'oracle.number.default-scale' is not set";
                } else if(roundMode.equals(RoundingMode.UNNECESSARY)) {
                    if (decimalDigits == 0) {
                        reason = String.format("scale isn't set (0) - unknown scale");
                    } else if (realPrecision > Decimals.MAX_PRECISION) {
                        reason = String.format("precision exceeds Presto MAX_PRECISION of %d",
                                Decimals.MAX_PRECISION);
                    } else if (absScale > Decimals.MAX_PRECISION) {
                        reason = String.format("scale exceeds Presto MAX_PRECISION of %d",
                                Decimals.MAX_PRECISION);
                    } else {
                        unsupported = false;
                    }
                } else {
                    unsupported = false;
                }

                if(unsupported) {
                    LOG.info("!!!!!!!!!!!!!! in unsupported");
                    // The data-type is unsupported in the current configuration
                    reason = String.format("JDBC DataType Unsupported - %s(%d,%d) %s %n... You can change this behavior" +
                                    "by modifying the configuration parameters 'oracle.number.rounding-mode' and " +
                                    "'unsupported-type.handling-strategy'",
                            typeName,
                            decimalDigits,
                            columnSize,
                            reason);
                    switch(unsupportedMode) {
                        case IGNORE:
                            LOG.warn(reason);
                            break;
                        case FAIL:
                            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, reason);
                        case CONVERT_TO_VARCHAR:
                            readFunc = OracleReadMappings.stringDecimalReadMapping(createUnboundedVarcharType());
                            readType = Optional.of(readFunc);
                            break;
                    }
                } else {
                    LOG.info("!!!!!!!!!!!! in supported");
                    if(decimalDigits == 0 && undefinedNumberScaleAsInt == true) {
                        LOG.info("!!!!!!!!!!!! in integerReadMapping");
                        readFunc = integerReadMapping();
                        readType = Optional.of(readFunc);
                    } else {
                        LOG.info("!!!!!!!!!!!! in supported roundDecimalReadMapping");
                        // This is the 'normal' happy path, use our custom ReadMapping function to parse the decimal value
                        // or round the value to the needed scale
                        if(logicalScale == 0) {
                            logicalScale = defaultScale;
                        }
                        readFunc = OracleReadMappings.roundDecimalReadMapping(
                                createDecimalType(logicalPrecision, logicalScale), realPrecision, roundMode);
                        readType = Optional.of(readFunc);
                    }
                }
                break;
            default:
                // use standard read mappings
                readType = jdbcTypeToPrestoType(typeHandle);
        }

        if(readType.isPresent()) { // The readType is empty
            // data type was unhandled
            int precision = typeHandle.getColumnSize();
            int scale = typeHandle.getDecimalDigits();
            int typeCode = typeHandle.getJdbcType();
            String typeName = JDBCType.valueOf(typeCode).getName();
            LOG.warn("Unhandled Column Type %s:%s [%s, %s] (precision, scale)", typeName, typeCode, precision, scale);
        }

        return readType;
    }
}
