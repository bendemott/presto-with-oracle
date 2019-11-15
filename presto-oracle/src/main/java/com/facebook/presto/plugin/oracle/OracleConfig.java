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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Decimals;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.math.RoundingMode;
import javax.validation.constraints.Min;
import java.sql.JDBCType;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;;

public class OracleConfig
{
    public static final JDBCType UNDEFINED_TYPE = JDBCType.OTHER;
    public static final int UNDEFINED_SCALE = OracleJdbcTypeHandle.UNDEFINED_SCALE;
    private static final JDBCType[] ALLOWED_NUMBER_DEFAULT_TYPES = {JDBCType.DECIMAL, JDBCType.DOUBLE, JDBCType.INTEGER};

    private boolean synonymsEnabled = false;
    private UnsupportedTypeHandling typeStrategy = UnsupportedTypeHandling.IGNORE;
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private UnsupportedTypeHandling numberExceedsLimitsMode = UnsupportedTypeHandling.ROUND;
    private List<OracleJdbcTypeHandle> numberAsIntegerTypes = new ArrayList<>();
    private List<OracleJdbcTypeHandle> numberAsDoubleTypes = new ArrayList<>();
    private List<OracleJdbcTypeHandle> numberAsDecimalTypes = new ArrayList<>();
    private JDBCType numberTypeDefault = JDBCType.DECIMAL;
    private JDBCType numberZeroScaleType = UNDEFINED_TYPE;
    private JDBCType numberNullScaleType = UNDEFINED_TYPE;
    private RoundingMode numberDecimalRoundMode = RoundingMode.UNNECESSARY;
    private int numberDecimalDefaultScaleFixed = UNDEFINED_SCALE;
    private float numberDecimalDefaultScaleRatio = UNDEFINED_SCALE;
    private Map<OracleJdbcTypeHandle, OracleJdbcTypeHandle> numberDecimalPrecisionMap = new HashMap<>();

    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }

    @Config("oracle.synonyms.enabled")
    public OracleConfig setSynonymsEnabled(boolean synonymsEnabled)
    {
        this.synonymsEnabled = synonymsEnabled;
        return this;
    }

    /** Get unsupported-type.handling-strategy */
    public UnsupportedTypeHandling getUnsupportedTypeStrategy()
    {
        return typeStrategy;
    }

    /**
     * Determines how types that are not supported by Presto natively are handled.
     * Oracle supports custom user defined types, so this could be anything.
     *
     * @param typeStrategy
     * @return
     */
    @Config("unsupported-type.handling-strategy")
    public OracleConfig setUnsupportedTypeStrategy(String typeStrategy)
    {
        typeStrategy = typeStrategy.toUpperCase();
        this.typeStrategy = UnsupportedTypeHandling.valueOf(typeStrategy);
        if(typeStrategy.equals(UnsupportedTypeHandling.ROUND)) {
            throw new PrestoException(CONFIGURATION_INVALID, "ROUND is not a valid option for unsupported-type.handling-strategy");
        }
        return this;
    }

    /** Get oracle.auto-reconnect */
    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("oracle.auto-reconnect")
    public OracleConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    /** Get oracle.max-reconnects */
    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("oracle.max-reconnects")
    public OracleConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    /** Get oracle.connection-timeout */
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("oracle.connection-timeout")
    public OracleConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /** Get oracle.number.exceeds-limits */
    public UnsupportedTypeHandling getNumberExceedsLimitsMode()
    {
        return numberExceedsLimitsMode;
    }

    /**
     * Configure the driver to handle Oracle NUMBER type that exceeds Presto DECIMAL type size limits.
     * The default behavior is to ROUND.
     *
     * If the rounding fails, or there is some other conversion exception, behavior falls-back to
     * "unsupported-type.handling-strategy"
     *
     * @param mode One of "ROUND", "CONVERT_TO_VARCHAR", "IGNORE", or "ERROR"
     * @return
     */
    @Config("oracle.number.exceeds-limits")
    public OracleConfig setNumberExceedsLimitsMode(String mode)
    {
        mode = mode.toUpperCase();
        this.numberExceedsLimitsMode = UnsupportedTypeHandling.valueOf(mode);
        return this;
    }


    // ------------------------------------------------------------------------
    // -- oracle.number.type-as fields ----------------------------------------
    /** Get oracle.number.type.as-integer */
    public List<OracleJdbcTypeHandle> getNumberAsIntegerTypes()
    {
        return numberAsIntegerTypes; // TODO
    }

    /**
     * These (PRECISION:SCALE) combinations will be treated as integers
     * @param precisions
     * @return
     */
    @Config("oracle.number.type.as-integer")
    public OracleConfig setNumberAsIntegerTypes(String precisions)
    {
        return this; // TODO
    }

    // ------------------------------------------------------------------------

    /** Get oracle.number.type.as-double*/
    public List<OracleJdbcTypeHandle> getNumberAsDoubleTypes()
    {
        return numberAsDoubleTypes; // TODO
    }

    /**
     * These (PRECISION:SCALE) combinations will be treated as double
     * @param precisions
     * @return
     */
    @Config("oracle.number.type.as-double")
    public OracleConfig setNumberAsDoubleTypes(String precisions)
    {
        return this; // TODO
    }

    // ------------------------------------------------------------------------

    /** Get oracle.number.type.as-decimal*/
    public List<OracleJdbcTypeHandle> getNumberAsDecimalTypes()
    {
        return numberAsDecimalTypes; // TODO
    }

    /**
     * These (PRECISION:SCALE) combinations will be treated as decimal
     * @param precisions
     * @return
     */
    @Config("oracle.number.type.as-decimal")
    public OracleConfig setNumberAsDecimalTypes(String precisions)
    {
        return this; // TODO
    }

    // ------------------------------------------------------------------------
    // -- oracle.number.type fields -------------------------------------------

    /** Get oracle.number.type.default */
    public JDBCType getNumberTypeDefault()
    {
        return numberTypeDefault;
    }

    @Config("oracle.number.type.default")
    public OracleConfig setNumberTypeDefault(String typeName)
    {
        numberTypeDefault = getNumericType(typeName);
        return this;
    }

    // ------------------------------------------------------------------------

    /** Get oracle.number.type.zero-scale-type */
    public JDBCType getNumberZeroScaleType()
    {
        return numberZeroScaleType;
    }

    /**
     * Oracle NUMBER types with a scale set to ZERO should be treated as this datatype.
     * This overrides "oracle.number.type.default"
     *
     * @param typeName
     * @return
     */
    @Config("oracle.number.type.zero-scale-type")
    public OracleConfig setNumberZeroScaleType(String typeName)
    {
        numberZeroScaleType = getNumericType(typeName);
        return this;
    }

    // ------------------------------------------------------------------------

    /** Get oracle.number.type.null-scale-type */
    public JDBCType getNumberNullScaleType()
    {
        return numberNullScaleType;
    }

    /**
     * Oracle Number Handling - When scale is NULL, what type should be mapped to
     * Default behavior is to NOT map to any scale
     * This overrides "oracle.number.type.default"
     *
     * Defaults to UNDEFINED_TYPE
     *
     * @param typeName
     * @return
     */
    @Config("oracle.number.type.null-scale-type")
    public OracleConfig setNumberNullScaleType(String typeName)
    {
        numberNullScaleType = getNumericType(typeName);
        return this;
    }

    // ---------------------------------------------------------------------------
    // -- oracle.number.decimal fields -------------------------------------------

    /** Get oracle.number.decimal.round-mode */
    public RoundingMode getNumberDecimalRoundMode()
    {
        if(getNumberExceedsLimitsMode().equals(UnsupportedTypeHandling.ROUND)
                && numberDecimalRoundMode.equals(RoundingMode.UNNECESSARY)) {
            throw new PrestoException(CONFIGURATION_INVALID, "'oracle.number.decimal.round-mode' must be set " +
                    "if 'oracle.number.exceeds-limits' is set to ROUND");
        }
        return numberDecimalRoundMode;
    }

    /**
     * When "oracle.number.exceeds-limits" is set to "ROUND" this must be set to a value other than UNNECESSARY.
     * This determines the method of rounding to conform values to Presto data type limits.
     *
     * @param roundingMode
     * @return
     */
    @Config("oracle.number.decimal.round-mode")
    public OracleConfig setNumberDecimalRoundMode(String roundingMode)
    {
        this.numberDecimalRoundMode = RoundingMode.valueOf(roundingMode);
        return this;
    }

    // ------------------------------------------------------------------------
    // -- oracle.number.decimal.default-scale fields --------------------------

    /** Get oracle.number.decimal.default-scale.fixed */
    public int getNumberDecimalDefaultScaleFixed()
    {
        return numberDecimalDefaultScaleFixed;
    }

    /**
     * When NUMBER type scale is NULL and conversion to DECIMAL is set,
     * EXPLICITLY set the scale to a fixed integer value.
     *
     * Note that scale cannot exceed Prestos maximum precision of 38
     *
     * @param numberScale
     * @return
     */
    @Config("oracle.number.decimal.default-scale.fixed")
    public OracleConfig setNumberDecimalDefaultScaleFixed(int numberScale)
    {
        if(numberScale > Decimals.MAX_PRECISION) {
            String msg = String.format("oracle.number.decimal.default-scale.fixed (%d) exceeds Prestos max: %d", numberScale, Decimals.MAX_PRECISION);
            throw new PrestoException(CONFIGURATION_INVALID, msg);
        }
        this.numberDecimalDefaultScaleFixed = numberScale;
        return this;
    }

    // ------------------------------------------------------------------------

    /** Get oracle.number.decimal.default-scale.ratio */
    public float getNumberDecimalDefaultScaleRatio()
    {
        return numberDecimalDefaultScaleRatio;
    }

    /**
     * When NUMBER type scale is NULL and conversion to DECIMAL is set,
     * use a ratio-based scale.
     * The scale will be set as a fraction of the precision.
     *
     * Note that scale is only applied to data-types in Oracle in which scale has been left undefined.
     *
     * Examples of (precision, scale) combinations and what they will be mapped to given an input
     *
     * 0.5 => (40, null) => (38, 19)   // scale of 40 is capped at 38, half of 38 is 19.
     * 0.5 => (16, null) => (16, 8)
     * 0.5 => (16, 0)    => scale is set, ignored
     * 0.2 => (38, null) => (38, 8)
     * 0.3 => (14, null) => (14, 4)
     *
     * @param numberScale
     * @return
     */
    @Config("oracle.number.decimal.default-scale.ratio")
    public OracleConfig setNumberDecimalDefaultScaleRatio(float numberScale)
    {
        if(getNumberDecimalDefaultScaleFixed() != UNDEFINED_SCALE) {
            String msg = String.format("oracle.number.decimal.default-scale.fixed is set, and conflicts with oracle.number.decimal.default-scale.ratio");
            throw new PrestoException(CONFIGURATION_INVALID, msg);
        }
        if(numberScale >  1.0f) {
            String msg = String.format("oracle.number.decimal.default-scale.ratio (%f) exceeds 1.0", numberScale, Decimals.MAX_PRECISION);
            throw new PrestoException(CONFIGURATION_INVALID, msg);
        }
        this.numberDecimalDefaultScaleRatio = numberScale;
        return this;
    }

    // ------------------------------------------------------------------------

    /** Get oracle.number.decimal.precision-map */
    public Map<OracleJdbcTypeHandle, OracleJdbcTypeHandle> getNumberDecimalPrecisionMap()
    {
        return numberDecimalPrecisionMap;
    }

    /**
     * Map (precision:scale) combinations to explicit precisions and scales.
     * The input precision:scale values are the values that come from the database, use "null" to represent and
     * undefined precision or scale.
     * The output precision:scale pair will be the data type that is used within Presto
     * @param precisions a string such as "null:null=38:12, 16:8=38:14"
     * @return
     */
    @Config("oracle.number.decimal.precision-map")
    public OracleConfig setNumberDecimalPrecisionMap(String precisions)
    {
        if(precisions == null || precisions.isEmpty()) {
            return this;
        }
        return this;
        // TODO
        //throw new PrestoException(CONFIGURATION_INVALID, "NOT IMPLEMENTED YET");
        //return this;
    }

    // ------------------------------------------------------------------------

    /**
     * Get JDBC numeric type or throw PrestoException if typeName passed is unsupported
     *
     * @param typeName String JDBCType name (case does not matter) - DECIMAL, INTEGER, DOUBLE
     */
    private JDBCType getNumericType(String typeName)
    {
        typeName = typeName.toUpperCase();
        JDBCType jdbcType = JDBCType.valueOf(typeName);
        boolean isAllowedType = Arrays.stream(ALLOWED_NUMBER_DEFAULT_TYPES).anyMatch(jdbcType::equals);
        if(!isAllowedType) {
            List<String> allowedVals = Arrays.stream(ALLOWED_NUMBER_DEFAULT_TYPES)
                    .map(JDBCType::getName)
                    .collect(Collectors.toList());

            throw new PrestoException(CONFIGURATION_INVALID,
                    String.format("'%s' is not valid for oracle.number.type.default %nAllowed Values: %s", typeName, allowedVals));

        }
        return jdbcType;
    }

    /**
     * IS
     */
    public boolean isDefaultScaleConfigured() {
        return (getNumberDecimalDefaultScaleFixed() != UNDEFINED_SCALE ||
                getNumberDecimalDefaultScaleRatio() != UNDEFINED_SCALE);
        // TODO
        //return false;
    }

}
