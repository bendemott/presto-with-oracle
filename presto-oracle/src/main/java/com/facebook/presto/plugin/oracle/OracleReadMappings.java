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

import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import io.airlift.log.Logger;

import static io.airlift.slice.Slices.utf8Slice;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Methods that deal with Oracle specific functionality around ReadMappings.
 * ReadMappings are methods returned to Presto to convert data types for specific columns in a JDBC result set.
 * These methods convert JDBC types to Presto supported types.
 * This logic is used in OracleNumberHandling.java
 */
public class OracleReadMappings {
    /**
     * ReadMapping that rounds decimals and sets PRECISION and SCALE explicitly.
     *
     * In the event the Precision of a NUMERIC or DECIMAL from Oracle exceeds the supported precision of Presto's
     * Decimal Type, we will ROUND / Truncate the Decimal Type.
     *
     * @param decimalType
     * @param round
     * @return
     */
    public static ReadMapping roundDecimalReadMapping(DecimalType decimalType, RoundingMode round) {
        Objects.requireNonNull(decimalType, "decimalType is null");
        Objects.requireNonNull(round, "round is null");
        return ReadMapping.sliceReadMapping(decimalType, (resultSet, columnIndex) -> {
            int scale = decimalType.getScale();
            BigDecimal dec = resultSet.getBigDecimal(columnIndex);
            // if value is null, return null
            if(dec == null) { return null; }

            // round will add zeros, or truncate by rounding to ensure the digits to the right of the decimal
            // are filled to exactly SCALE digits.
            dec = dec.setScale(scale, round);
            return Decimals.encodeUnscaledValue(dec.unscaledValue());
        });
    }

    /**
     * Return a Double rounded to the desired scale
     *
     * @param scale
     * @param round
     * @return
     */
    public static ReadMapping roundDoubleReadMapping(int scale, RoundingMode round) {
        Objects.requireNonNull(round, "round is null");
        return ReadMapping.doubleReadMapping(DoubleType.DOUBLE, (resultSet, columnIndex) -> {
            BigDecimal value = resultSet.getBigDecimal(columnIndex);
            value = value.setScale(scale, round); // round to ensure the decimal value will fit in a double
            return value.doubleValue();
        });
    }

    /**
     * Convert decimal type of unknown precision to unbounded varchar
     *
     * @param varcharType
     * @return
     */
    public static ReadMapping decimalVarcharReadMapping(VarcharType varcharType) {
        return ReadMapping.sliceReadMapping(varcharType, (resultSet, columnIndex) -> {
            BigDecimal dec = resultSet.getBigDecimal(columnIndex);
            if(dec == null) { return null; }
            return utf8Slice(dec.toString());
        });
    }
}