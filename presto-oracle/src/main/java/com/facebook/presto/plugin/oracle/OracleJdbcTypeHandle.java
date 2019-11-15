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

import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.type.Decimals;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.Objects;

/**
 * Oracle custom JdbcTypeHandle
 *
 * Justification:
 *  This class is required because Oracle makes a distinction of scale/precision 0 vs null that is reflected in a
 *  special way.
 *
 *  Oracle returns -127 when scale is NULL (undefined) internally within Oracle for a DECIMAL data type.
 *
 *  For this reason we've added explicit methods to this class and use it in decimal handling code to make a complex
 *  set of options much easier to follow.
 */
public class OracleJdbcTypeHandle {
    public static final String NULL_VALUE = "null";
    public static final int UNDEFINED_SCALE = -127;
    private int jdbcType;
    private final int columnSize;
    private final int decimalDigits;
    private int scale;
    private int precision;
    private boolean precisionUndefined = false;
    private boolean scaleUndefined = false;
    private boolean scaleLimitExceeded = false;
    private boolean precisionLimitedExceeded = false;

    /**
     *
     * @param jdbcType jdbcType (sql.Types)
     * @param columnSize COLUMN_WIDTH from JDBC Driver (precision for numeric types)
     * @param decimalDigits Decimal Digits from JDBC Driver (represents scale) can be negative
     */
    @JsonCreator
    public OracleJdbcTypeHandle(@JsonProperty("jdbcType") int jdbcType, @JsonProperty("columnSize") int columnSize, @JsonProperty("decimalDigits") int decimalDigits) {
        this.jdbcType = jdbcType;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.precision = columnSize;
        this.scale = decimalDigits;

        if(jdbcType != Types.NUMERIC) {
            return;
        }

        // -- Special logic around DECIMAL SCALE ------------------------------
        int addPrecision = 0;
        if(decimalDigits == UNDEFINED_SCALE) {
            this.scaleUndefined = true;
        } else if (decimalDigits < 0) {
            precisionLimitedExceeded = true;
            this.scale = 0;
        } else if(decimalDigits > Decimals.MAX_PRECISION) {
            scaleLimitExceeded = true;
        }

        // -- Special logic around DECIMAL PRECISION --------------------------
        this.precision += addPrecision;
        if(columnSize == 0) {
            precisionUndefined = true;
        } else if (precision > Decimals.MAX_PRECISION) {
            precisionLimitedExceeded = true;
        }

    }

    public OracleJdbcTypeHandle(JdbcTypeHandle handle) {
        this(handle.getJdbcType(), handle.getColumnSize(), handle.getDecimalDigits());
    }

    public OracleJdbcTypeHandle(OracleJdbcTypeHandle handle) {
        this(handle.getJdbcType(), handle.getColumnSize(), handle.getDecimalDigits());
    }

    @JsonProperty
    public int getJdbcType() {
        return this.jdbcType;
    }

    public OracleJdbcTypeHandle setJdbcType(int jdbcType) {
        JDBCType.valueOf(jdbcType);
        this.jdbcType = jdbcType;
        return this;
    }

    public JDBCType getEnumType() {
        return JDBCType.valueOf(this.jdbcType);
    }

    @JsonProperty
    public int getColumnSize() {
        return this.columnSize;
    }

    @JsonProperty
    public int getDecimalDigits() {
        return this.decimalDigits;
    }

    public int getPrecision() {
        return this.precision;
    }

    public OracleJdbcTypeHandle setPrecision(int precision) {
        this.precision = precision;
        return this;
    }

    public int getScale() {
        return this.scale;
    }

    public OracleJdbcTypeHandle setScale(int scale) {
        this.scale = scale;
        return this;
    }

    public boolean isTypeLimitExceeded() {
        return this.scaleLimitExceeded || this.precisionLimitedExceeded;
    }

    public boolean isScaleLimitExceeded() {
        return this.scaleLimitExceeded;
    }

    public boolean isPrecisionLimitExceeded() {
        return this.precisionLimitedExceeded;
    }

    public boolean isPrecisionUndefined() {
        return this.precisionUndefined;
    }

    public boolean isScaleUndefined() {
        return this.scaleUndefined;
    }

    public String getDescription() {
        String typeName = getTypeName();
        switch (jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return String.format("%s(%d, %d)", typeName, precision, scale);
            default:
                return String.format("%s(%d)", typeName, columnSize);
        }
    }

    public String getTypeName() {
        return JDBCType.valueOf(jdbcType).getName();
    }

    public String getPrecisionDesc() {
        String precisionVal = isPrecisionUndefined() ? NULL_VALUE : Integer.toString(precision);
        String scaleVal = isScaleUndefined() ? NULL_VALUE : Integer.toString(scale);
        return String.format("%s:%s", precisionVal, scaleVal);
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.jdbcType, this.columnSize, this.decimalDigits});
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            OracleJdbcTypeHandle that = (OracleJdbcTypeHandle)o;
            if(this.jdbcType == Types.NUMERIC) {
                return (this.jdbcType == that.jdbcType
                        && this.precision == that.precision
                        && this.scale == that.scale);
            } else {
                return (this.jdbcType == that.jdbcType
                        && this.columnSize == that.columnSize
                        && this.decimalDigits == that.decimalDigits);
            }

        } else {
            return false;
        }
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("jdbcType", this.jdbcType).add("columnSize", this.columnSize).add("decimalDigits", this.decimalDigits).toString();
    }
}
