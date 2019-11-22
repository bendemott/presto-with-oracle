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

import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.plugin.jdbc.StandardReadMappings;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import io.airlift.log.Logger;

import java.math.RoundingMode;
import java.sql.JDBCType;
import java.sql.Types;

import static com.google.common.base.Preconditions.checkArgument;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.min;
/**
 * The Oracle NUMBER type is a variadic type that can behave like an INTEGER, or arbitrary precision DECIMAL
 *
 * Like any decimal, number supports a PRECISION (the total number of digits supported) and a SCALE
 * (the total number of digits to the right of the decimal)
 *
 * This class considers all Oracle specific options around type-handling and properly infers the way a numeric
 * type should be represented within presto.
 *
 *
 */
public class OracleNumberHandling {
    private JDBCType mapToType;
    private ReadMapping readMapping;
    private static final Logger LOG = Logger.get(OracleNumberHandling.class);

    public OracleNumberHandling(OracleJdbcTypeHandle typeHandle, OracleConfig config) {

        checkArgument(typeHandle != null, "typeHandle cannot be null");
        checkArgument(config != null, "config cannot be null");

        // Map NUMBER to the default type based on
        //  oracle.number.type.default
        mapToType = config.getNumberTypeDefault();

        // If scale is undefined, and configuration exists to cast it to a specific type
        // based on "oracle.number.type.null-scale-type"
        if(typeHandle.isScaleUndefined() && !config.getNumberNullScaleType().equals(OracleConfig.UNDEFINED_TYPE)) {
            mapToType = config.getNumberNullScaleType();
        }

        // If scale is zero, and configuration exists to cast it to a specific type
        // based on "oracle.number.type.zero-scale-type"
        if(typeHandle.getScale() == 0 && !config.getNumberZeroScaleType().equals(OracleConfig.UNDEFINED_TYPE)) {
            mapToType = config.getNumberZeroScaleType();
        }

        // If we're mapping to DECIMAL, and the JDBC Column Type exceeds the limits of DECIMAL perform action
        // based on "oracle.number.exceeds-limits"
        if(mapToType.equals(JDBCType.DECIMAL) && typeHandle.isTypeLimitExceeded()) {
            switch(config.getNumberExceedsLimitsMode()) {
                case ROUND:
                    break;
                case VARCHAR:
                    mapToType = JDBCType.VARCHAR;
                    break;
                case IGNORE:
                    throw new IgnoreFieldException(String.format("IGNORING type exceeds limits: %s, you can configure " +
                            "'oracle.number.exceeds-limits' to change behavior", typeHandle.getDescription()));
                case FAIL:
                    throw new PrestoException(
                            StandardErrorCode.GENERIC_INTERNAL_ERROR,
                            String.format("type exceeds limits: %s, you can configure " +
                                    "'oracle.number.exceeds-limits' to change behavior", typeHandle.getDescription()));
            }
        }

        // Handle explicit mapping of (precision, scale) combinations to specific data types
        // based on the configuration values:
        //     "oracle.number.type.as-integer"
        //     "oracle.number.type.as-double"
        //     "oracle.number.type.as-decimal"
        if(!mapToType.equals(JDBCType.INTEGER) && config.getNumberAsIntegerTypes().contains(typeHandle)) {
            mapToType = JDBCType.INTEGER;
        } else if (!mapToType.equals(JDBCType.DOUBLE) && config.getNumberAsDoubleTypes().contains(typeHandle)) {
            mapToType = JDBCType.DOUBLE;
        } else if (!mapToType.equals(JDBCType.DECIMAL) && config.getNumberAsDecimalTypes().contains(typeHandle)) {
            mapToType = JDBCType.DECIMAL;
        }

        // HANDLE DECIMAL READ MAPPING
        // ====================================================================
        if(mapToType.equals(JDBCType.DECIMAL)) {
            typeHandle.setJdbcType(Types.DECIMAL); // not needed, but avoids confusion (NUMERIC) won't be shown.
            OracleJdbcTypeHandle readHandle;

            // if there is a mapping to an explicit (precision:scale) pair use it
            //    from "oracle.number.decimal.precision-map"
            readHandle = config.getNumberDecimalPrecisionMap().getOrDefault(typeHandle, null);

            // If the scale exceeds precision, or precision is undefined, or the precision exceeds the max
            // set the precision to the Maximum allowed precision
            if(readHandle == null && (typeHandle.getScale() >= typeHandle.getPrecision()
                    || typeHandle.isPrecisionUndefined()
                    || typeHandle.isTypeLimitExceeded())) {
                typeHandle.setPrecision(Decimals.MAX_PRECISION);
            }

            // if the scale is undefined (-127) or exceeds max precision...
            // use one of two configuration options to decide what to do
            //    "oracle.number.decimal.default-scale.fixed"
            //    "oracle.number.decimal.default-scale.ratio"
            if(readHandle == null && (typeHandle.isScaleUndefined() || typeHandle.isScaleLimitExceeded())) {
                // If a fixed-scale is configured, apply the fixed scale.
                if(config.getNumberDecimalDefaultScaleFixed() != OracleConfig.UNDEFINED_SCALE) {
                    int scale = config.getNumberDecimalDefaultScaleFixed();
                    readHandle = new OracleJdbcTypeHandle(typeHandle);
                    readHandle.setScale(scale);
                // if a ratio-scale is configured, apply the ratio scale
                } else if(config.getNumberDecimalDefaultScaleRatio() != (float) OracleConfig.UNDEFINED_SCALE) {
                    float ratio = config.getNumberDecimalDefaultScaleRatio();
                    int scale = (int) (ratio * (float) min(typeHandle.getPrecision(), Decimals.MAX_PRECISION));
                    readHandle = new OracleJdbcTypeHandle(typeHandle);
                    readHandle.setScale(scale);
                } else {
                    throw new PrestoException(
                            StandardErrorCode.GENERIC_INTERNAL_ERROR,
                            String.format("type has no scale: %s, and no default scale is set via " +
                                    "'oracle.number.decimal.default-scale.fixed' or" +
                                    "'oracle.number.decimal.default-scale.ratio' or " +
                                    "'oracle.number.decimal.precision-map'", typeHandle.getDescription()));
                }
                // finally if the new scale that has been set exceeds the precision, max-out-precision
                if(readHandle.getPrecision() <= readHandle.getScale()) {
                    readHandle.setPrecision(Decimals.MAX_PRECISION);
                }
            } else if(readHandle == null) {
                readHandle = new OracleJdbcTypeHandle(typeHandle);
            }

            DecimalType prestoDecimal = createDecimalType(readHandle.getPrecision(), readHandle.getScale());
            if(typeHandle.isTypeLimitExceeded() || typeHandle.isPrecisionUndefined() || typeHandle.isScaleUndefined()) {
                // if the type exceeds limits, or is undefined in precision or scale, we might have to round.
                // so return the rounding version of the read function
                readMapping = OracleReadMappings.roundDecimalReadMapping(prestoDecimal, config.getNumberDecimalRoundMode());
            } else {
                // if the type is well-defined and falls within our limits, Presto's default read function can be used
                readMapping = StandardReadMappings.decimalReadMapping(prestoDecimal);
            }
        }

        // HANDLE INTEGER READ MAPPING
        // ====================================================================
        else if(mapToType.equals(JDBCType.INTEGER)) {
            readMapping = StandardReadMappings.integerReadMapping();
        }

        // HANDLE DOUBLE READ MAPPING
        // ====================================================================
        else if(mapToType.equals(JDBCType.DOUBLE)) {
            RoundingMode round = config.getNumberDoubleRoundMode();
            int dblScale = config.getNumberDoubleDefaultScaleFixed();
            if(round == RoundingMode.UNNECESSARY || dblScale == OracleConfig.UNDEFINED_SCALE) {
                readMapping = StandardReadMappings.doubleReadMapping();
            } else {
                readMapping = OracleReadMappings.roundDoubleReadMapping(dblScale, round);
            }
        }

        // HANDLE VARCHAR READ MAPPING
        // ====================================================================
        else if(mapToType.equals(JDBCType.VARCHAR)) {
            readMapping = OracleReadMappings.decimalVarcharReadMapping(createUnboundedVarcharType());
        }

        // NO READ MAPPING MATCHED
        // ====================================================================
        else {
            throw new PrestoException(
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    String.format("unsupported type %s, for NumberHandling", typeHandle.getDescription()));
        }
    }

    /**
     * Get the JDBCType that the value will be mapped to based on configuration.
     * @return
     */
    JDBCType getMapToType() {
        return mapToType;
    }

    /**
     * Get the ReadMapping function for the given jdbc column type
     * @return
     */
    public ReadMapping getReadMapping() {
        return readMapping;
    }
}