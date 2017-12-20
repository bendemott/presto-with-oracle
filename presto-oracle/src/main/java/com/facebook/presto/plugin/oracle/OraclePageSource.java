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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class OraclePageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;
    private final RecordCursor cursor;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private boolean closed;

    public OraclePageSource(RecordSet recordSet)
    {
        this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
    }

    public OraclePageSource(List<Type> types, RecordCursor cursor)
    {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
        this.pageBuilder = new PageBuilder(this.types);
    }

    public RecordCursor getCursor()
    {
        return cursor;
    }

    @Override
    public long getCompletedBytes()
    {
        return cursor.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return cursor.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return cursor.getSystemMemoryUsage() + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
        cursor.close();
    }

    @Override
    public boolean isFinished()
    {
        return closed && pageBuilder.isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        if (!closed) {
            int i;
            for (i = 0; i < ROWS_PER_REQUEST; i++) {
                if (pageBuilder.isFull()) {
                    break;
                }

                if (!cursor.advanceNextPosition()) {
                    closed = true;
                    break;
                }

                pageBuilder.declarePosition();
                for (int column = 0; column < types.size(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    Type type = types.get(column);
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        Object object = cursor.getObject(column);
                        if (object == null) {
                            output.appendNull();
                        }
                        else {
                            type.writeBoolean(output, (Boolean) object);
                        }
                    }
                    else if (javaType == long.class) {
                        Object object = cursor.getObject(column);
                        if (object == null) {
                            output.appendNull();
                        }
                        else {
                            if (type.equals(TinyintType.TINYINT)) {
                                type.writeLong(output, (Long) object);
                            }
                            if (type.equals(SmallintType.SMALLINT)) {
                                type.writeLong(output, (Long) object);
                            }
                            if (type.equals(IntegerType.INTEGER)) {
                                type.writeLong(output, (Long) object);
                            }
                            if (type.equals(RealType.REAL)) {
                                type.writeLong(output, Float.floatToRawIntBits((Float) object));
                            }
                            if (type.equals(BigintType.BIGINT)) {
                                type.writeLong(output, (Long) object);
                            }
                            if (type instanceof DecimalType) {
                                // short decimal type
                                type.writeLong(output, ((BigDecimal) object).unscaledValue().longValueExact());
                            }
                            if (type.equals(DateType.DATE)) {
                                // JDBC returns a date using a timestamp at midnight in the JVM timezone
                                long localMillis = ((Date) object).getTime();
                                // Convert it to a midnight in UTC
                                long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
                                // convert to days
                                type.writeLong(output, TimeUnit.MILLISECONDS.toDays(utcMillis));
                            }
                            if (type.equals(TimeType.TIME)) {
                                type.writeLong(output, ISOChronology.getInstanceUTC().millisOfDay().get((Long) object));
                            }
                            if (type.equals(TimestampType.TIMESTAMP)) {
                                type.writeLong(output, ((Timestamp) object).getTime());
                            }
                        }
                    }
                    else if (javaType == double.class) {
                        Object object = cursor.getObject(column);
                        if (object == null) {
                            output.appendNull();
                        }
                        else {
                            type.writeDouble(output, (Double) object);
                        }
                    }
                    else if (javaType == Slice.class) {
                        Slice slice = cursor.getSlice(column);
                        if (slice == null) {
                            output.appendNull();
                        }
                        else {
                            type.writeSlice(output, slice, 0, slice.length());
                        }
                    }
                    else {
                        type.writeObject(output, cursor.getObject(column));
                    }
                }
            }
        }

        // only return a page if the buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    }
}
