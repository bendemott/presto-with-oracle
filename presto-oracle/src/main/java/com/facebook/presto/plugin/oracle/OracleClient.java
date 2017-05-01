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
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleDriver;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Locale.ENGLISH;

public class OracleClient
        extends BaseJdbcClient
{
    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OracleConfig oracleConfig)
            throws SQLException
    {
        super(connectorId, config, "`", new OracleDriver());
    }

    @Override
    public Set<String> getSchemaNames() {
        try (Connection connection = driver.connect(connectionUrl,
                connectionProperties);
             ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName) throws SQLException {
        return connection.getMetaData().getTables(null, schemaName, tableName,
                new String[] { "TABLE", "SYNONYM" });
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
        try (Connection connection = driver.connect(connectionUrl,
                connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase();
                jdbcTableName = jdbcTableName.toUpperCase();
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName,
                    jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(connectorId,
                            schemaTableName, resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"), resultSet
                            .getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED,
                            "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle) {
        try (Connection connection = driver.connect(connectionUrl,
                connectionProperties)) {

            ( (oracle.jdbc.driver.OracleConnection)connection ).setIncludeSynonyms(true);
            DatabaseMetaData metadata = connection.getMetaData();
            String schemaName = tableHandle.getSchemaName().toUpperCase();
            String tableName = tableHandle.getTableName().toUpperCase();
            try (ResultSet resultSet = metadata.getColumns(null, schemaName,
                    tableName, null)) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet
                            .getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId,
                                columnName, columnType));
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(
                            tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED,
                            "Table has no supported column types: "
                                    + tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema) {
        try (Connection connection = driver.connect(connectionUrl,
                connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
                schema = schema.toUpperCase();
            }
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList
                        .builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet) throws SQLException {
        String tableSchema = resultSet.getString("TABLE_SCHEM");
        String tableName = resultSet.getString("TABLE_NAME");
        if (tableSchema != null) {
            tableSchema = tableSchema.toLowerCase();
        }
        if (tableName != null) {
            tableName = tableName.toLowerCase();
        }
        return new SchemaTableName(tableSchema, tableName);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata) {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();
            //modify the rule of table name , beceuse the length of oracle table name is between 0 and 30
            String temporaryName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
            temporaryName = temporaryName.substring(0,29);
            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(quoted(catalog, schema, temporaryName))
                    .append(" (");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(new StringBuilder()
                        .append(quoted(columnName))
                        .append(" ")
                        .append(toSqlType(column.getType()))
                        .toString());
            }
            Joiner.on(", ").appendTo(sql, columnList.build());
            sql.append(")");

            execute(connection, sql.toString());

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    temporaryName,
                    connectionUrl,
                    fromProperties(connectionProperties));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle) {
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                //new table name needn't to be with catalog and schema
                .append(handle.getTableName());

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }


}
