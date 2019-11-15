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

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import static io.airlift.slice.Slices.utf8Slice;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Methods that deal with Oracle specific functionality around ReadMappings.
 * ReadMappings are methods returned to Presto to convert data types for specific columns in a JDBC result set.
 * These methods convert JDBC types to Presto supported types.
 * This logic is used in OracleClient.java
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
    public static ReadMapping roundDecimalPrecisionAndScale(DecimalType decimalType, RoundingMode round) {
        return ReadMapping.sliceReadMapping(decimalType, (resultSet, columnIndex) -> {
            int scale = decimalType.getScale();
            BigDecimal roundDec = resultSet.getBigDecimal(columnIndex);
            roundDec = roundDec.setScale(scale, round);
            //roundDec = roundDec.round(new MathContext(roundDec.precision(), round));
            return Decimals.encodeScaledValue(roundDec, scale);
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
            String stringDec = resultSet.getBigDecimal(columnIndex).toString();
            return utf8Slice(stringDec);
        });
    }
}