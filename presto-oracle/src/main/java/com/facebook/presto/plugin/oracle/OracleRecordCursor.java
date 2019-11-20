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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.*;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.facebook.presto.spi.ConnectorSession;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;
import static java.lang.Math.min;

public class OracleRecordCursor
        implements RecordCursor
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private final List<JdbcColumnHandle> columnHandles;

    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private boolean closed;

    public OracleRecordCursor(JdbcClient jdbcClient, ConnectorSession session, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));

        try {
            connection = jdbcClient.getConnection(JdbcIdentity.from(session), split);
            statement = jdbcClient.buildSql(connection, split, columnHandles);
            resultSet = statement.executeQuery();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        try {
            boolean result = resultSet.next();
            if (!result) {
                close();
            }
            return result;
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return resultSet.getBoolean(field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            Type type = getType(field);
            if (type.equals(TinyintType.TINYINT)) {
                return (long) resultSet.getByte(field + 1);
            }
            if (type.equals(SmallintType.SMALLINT)) {
                return (long) resultSet.getShort(field + 1);
            }
            if (type.equals(IntegerType.INTEGER)) {
                return (long) resultSet.getInt(field + 1);
            }
            if (type.equals(RealType.REAL)) {
                return (long) floatToRawIntBits(resultSet.getFloat(field + 1));
            }
            if (type.equals(BigintType.BIGINT)) {
                return resultSet.getLong(field + 1);
            }
            if (type instanceof DecimalType) {
                // short decimal type
                return resultSet.getBigDecimal(field + 1).unscaledValue().longValueExact();
            }
            if (type.equals(DateType.DATE)) {
                // JDBC returns a date using a timestamp at midnight in the JVM timezone
                long localMillis = resultSet.getDate(field + 1).getTime();
                // Convert it to a midnight in UTC
                long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
                // convert to days
                return TimeUnit.MILLISECONDS.toDays(utcMillis);
            }
            if (type.equals(TimeType.TIME)) {
                Time time = resultSet.getTime(field + 1);
                return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
            }
            if (type.equals(TimestampType.TIMESTAMP)) {
                Timestamp timestamp = resultSet.getTimestamp(field + 1);
                return timestamp.getTime();
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for long: " + type.getTypeSignature());
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return resultSet.getDouble(field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            Type type = getType(field);
            if (type instanceof VarcharType) {
                String string = resultSet.getString(field + 1);
                return string == null ? null : utf8Slice(string);
            }
            if (type instanceof CharType) {
                String string = resultSet.getString(field + 1);
                return string == null ? null : utf8Slice(CharMatcher.is(' ').trimTrailingFrom(string));
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                byte[] bytes = resultSet.getBytes(field + 1);
                return bytes == null ? null : wrappedBuffer(bytes);
            }
            if (type instanceof DecimalType) {
                // long decimal type
                // TODO
                /*
java.lang.ArithmeticException: Decimal overflow
        at com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.throwOverflowException(UnscaledDecimal128Arithmetic.java:1650)
        at com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.pack(UnscaledDecimal128Arithmetic.java:154)
        at com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal(UnscaledDecimal128Arithmetic.java:144)
        at com.facebook.presto.spi.type.Decimals.encodeUnscaledValue(Decimals.java:144)
        at com.facebook.presto.spi.type.Decimals.encodeScaledValue(Decimals.java:170)
        at com.facebook.presto.plugin.oracle.OracleRecordCursor.getSlice(OracleRecordCursor.java:219)
        at com.facebook.presto.plugin.oracle.OraclePageSource.getNextPage(OraclePageSource.java:183)
        at com.facebook.presto.operator.TableScanOperator.getOutput(TableScanOperator.java:250)
        at com.facebook.presto.operator.Driver.processInternal(Driver.java:379)
        at com.facebook.presto.operator.Driver.lambda$processFor$8(Driver.java:283)
        at com.facebook.presto.operator.Driver.tryWithLock(Driver.java:675)
        at com.facebook.presto.operator.Driver.processFor(Driver.java:276)
        at com.facebook.presto.execution.SqlTaskExecution$DriverSplitRunner.processFor(SqlTaskExecution.java:1077)
        at com.facebook.presto.execution.executor.PrioritizedSplitRunner.process(PrioritizedSplitRunner.java:162)
        at com.facebook.presto.execution.executor.TaskExecutor$TaskRunner.run(TaskExecutor.java:483)
        at com.facebook.presto.$gen.Presto_0_225_9e57310____20191115_180708_1.run(Unknown Source)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
                 */
                BigDecimal bigDecimal = resultSet.getBigDecimal(field + 1);
                if(bigDecimal != null && bigDecimal.precision() > Decimals.MAX_PRECISION) {
                    int scale = min(bigDecimal.scale(), 8); // TODO 8 is arbitrary
                    bigDecimal = bigDecimal.setScale(scale, RoundingMode.UP);
                }

                return bigDecimal == null ? null : encodeScaledValue(bigDecimal);
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for slice: " + type.getTypeSignature());
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return resultSet.getObject(field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");

        try {
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(field + 1);

            return resultSet.wasNull();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (Connection connection = this.connection;
             Statement statement = this.statement;
             ResultSet resultSet = this.resultSet) {
            // do nothing
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return Throwables.propagate(e);
    }
}
