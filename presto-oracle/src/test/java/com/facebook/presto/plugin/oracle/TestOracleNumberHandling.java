package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.sql.JDBCType;
import java.sql.Types;

import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.integerReadMapping;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
public class TestOracleNumberHandling
{
    // TODO test type-limit-exceed IGNORE, CONVERT_TO_VARCHAR, FAIL
    // TODO test that scale >= precision, set precision to MAX_PRECISION
    //
    @Test void testReadMapCompare()
    {
        DecimalType type1 = createDecimalType(Decimals.MAX_PRECISION, 2);
        ReadMapping read1 = OracleReadMappings.roundDecimalReadMapping(type1, RoundingMode.UP);
        DecimalType type2 = createDecimalType(Decimals.MAX_PRECISION, 2);
        ReadMapping read2 = OracleReadMappings.roundDecimalReadMapping(type2, RoundingMode.UP);
        DecimalType type3 = createDecimalType(Decimals.MAX_SHORT_PRECISION + 1, 2);
        ReadMapping read3 = OracleReadMappings.roundDecimalReadMapping(type3, RoundingMode.UP);

        TestOracleReadMappings.assertReadMappingEquals(read1, read2);
        TestOracleReadMappings.assertReadMappingNotEquals(read2, read3);

        // ensure our custom and default read mappings are not seen as equal, even with the same type parameters.
        ReadMapping read4 = StandardReadMappings.decimalReadMapping(createDecimalType(30, 2));
        ReadMapping read5 = OracleReadMappings.roundDecimalReadMapping(createDecimalType(30, 2), RoundingMode.UP);
        TestOracleReadMappings.assertReadMappingNotEquals(read4, read5);
    }

    @Test void testScaleSettingsConflict()
    {
        OracleConfig config = new OracleConfig();
        config.setNumberDecimalDefaultScaleFixed(OracleConfig.UNDEFINED_SCALE);
        config.setNumberDecimalDefaultScaleRatio(OracleConfig.UNDEFINED_SCALE);

        config.setNumberDecimalDefaultScaleFixed(8);
        assertThrows(PrestoException.class, () -> config.setNumberDecimalDefaultScaleRatio(0.3f));
    }

    @Test void testOracleRoundingModeThrows()
    {
        OracleConfig config = new OracleConfig();
        config.setNumberExceedsLimitsMode("ROUND");
        config.setNumberDecimalRoundMode("UNNECESSARY");
        assertThrows(PrestoException.class, () -> config.getNumberDecimalRoundMode());
        assertThrows(IllegalArgumentException.class, () -> config.setNumberExceedsLimitsMode("ASDF"));
        assertThrows(IllegalArgumentException.class, () -> config.setNumberDecimalRoundMode("ASDF"));
    }

    @Test
    public void testUndefinedScaleDefault()
    {
        OracleConfig config = buildConfig();
        config.setNumberNullScaleType("DECIMAL");
        config.setNumberZeroScaleType("INTEGER");
        OracleNumberHandling numberHandling;

        numberHandling = buildNumberHandling(Decimals.MAX_PRECISION, 2, config);
        assertEquals(numberHandling.getMapToType(), JDBCType.DECIMAL);
    }
    @Test
    public void testDecimalMappingRatio()
    {
        int expectedScale;
        int precision;
        float ratioScale = 0.4f;
        OracleConfig config = buildConfig();
        config.setNumberDecimalDefaultScaleFixed(OracleConfig.UNDEFINED_SCALE);
        config.setNumberDecimalDefaultScaleRatio(ratioScale);

        OracleNumberHandling numberHandling;
        ReadMapping readExpected;
        RoundingMode round = config.getNumberDecimalRoundMode();

        // ensure we can get the ratio scale and its set properly.
        assertEquals(ratioScale, config.getNumberDecimalDefaultScaleRatio());

        // test (ratio) defined precision, defined scale (within limits)
        /*
        numberHandling = buildNumberHandling(30, 2, config);
        readExpected = decimalReadMapping(createDecimalType(30, 2));
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());
         */

        // test (ratio) defined precision, undefined scale
        precision = 30;
        expectedScale = (int) (ratioScale * (float) precision);
        numberHandling = buildNumberHandling(precision, OracleConfig.UNDEFINED_SCALE, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(createDecimalType(precision, expectedScale), round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // test (ratio) undefined precision, defined scale
        expectedScale = 2;
        numberHandling = buildNumberHandling(0, expectedScale, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(createDecimalType(Decimals.MAX_PRECISION, expectedScale), round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // test (ratio) undefined precision, undefined scale
        expectedScale = (int) (ratioScale * (float) Decimals.MAX_PRECISION);
        numberHandling = buildNumberHandling(0, OracleConfig.UNDEFINED_SCALE, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(
                createDecimalType(Decimals.MAX_PRECISION, expectedScale),
                round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // test (ratio) scale >= precision, and scale <= MAX_PRECISION
        /*
        expectedScale = 24;
        numberHandling = buildNumberHandling(20, expectedScale, config);
        readExpected = decimalReadMapping(createDecimalType(Decimals.MAX_PRECISION, expectedScale));
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());*/

        // test (ratio) scale >= precision, and scale > MAX_PRECISION
        // in this case because the scale exceeds precision, precision will be set to MAX_PRECISION
        precision = 20;
        expectedScale = (int) (ratioScale * (float) Decimals.MAX_PRECISION);
        numberHandling = buildNumberHandling(precision, Decimals.MAX_PRECISION + 1, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(
                createDecimalType(Decimals.MAX_PRECISION,  expectedScale),
                round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // todo test negative precision
    }

    @Test
    public void testDecimalMappingFixed()
    {
        int fixedScale = 8;
        OracleConfig config = buildConfig();
        config.setNumberDecimalDefaultScaleFixed(fixedScale);

        OracleNumberHandling numberHandling;
        ReadMapping readExpected;
        RoundingMode round = config.getNumberDecimalRoundMode();

        assertEquals(fixedScale, config.getNumberDecimalDefaultScaleFixed());

        // test defined precision, defined scale (within limits)
        /*
        numberHandling = buildNumberHandling(30, 2, config);
        readExpected = decimalReadMapping(createDecimalType(30, 2));
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());*/

        // test defined precision, undefined scale
        numberHandling = buildNumberHandling(Decimals.MAX_PRECISION, OracleConfig.UNDEFINED_SCALE, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(createDecimalType(Decimals.MAX_PRECISION, fixedScale), round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // test undefined precision, defined scale
        fixedScale = 2;
        numberHandling = buildNumberHandling(0, fixedScale, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(createDecimalType(Decimals.MAX_PRECISION, fixedScale), round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // test undefined precision, undefined scale
        numberHandling = buildNumberHandling(0, OracleConfig.UNDEFINED_SCALE, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(
                createDecimalType(Decimals.MAX_PRECISION, config.getNumberDecimalDefaultScaleFixed()),
                round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());

        // test scale >= precision, and scale <= MAX_PRECISION
        /*
        fixedScale = 24;
        numberHandling = buildNumberHandling(20, fixedScale, config);
        readExpected = decimalReadMapping(createDecimalType(Decimals.MAX_PRECISION, fixedScale));
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());*/

        // test scale >= precision, and scale > MAX_PRECISION
        fixedScale = Decimals.MAX_PRECISION + 1;
        numberHandling = buildNumberHandling(20, fixedScale, config);
        readExpected = OracleReadMappings.roundDecimalReadMapping(
                createDecimalType(Decimals.MAX_PRECISION, config.getNumberDecimalDefaultScaleFixed()),
                round);
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());
    }

    @Test void testDecimalZeroScaleAsInteger()
    {
        OracleConfig config = buildConfig();
        OracleNumberHandling numberHandling = buildNumberHandling(Decimals.MAX_PRECISION, 0, config);
        assertEquals(numberHandling.getMapToType(), JDBCType.INTEGER);
        ReadMapping readExpected = integerReadMapping();
        TestOracleReadMappings.assertReadMappingEquals(readExpected, numberHandling.getReadMapping());
    }

    @Test void testDecimalNullScaleAsDecimalWithFixed()
    {
        int fixedScale = 8;

        OracleConfig config = buildConfig();
        config.setNumberDecimalDefaultScaleRatio(OracleConfig.UNDEFINED_SCALE);
        config.setNumberDecimalDefaultScaleFixed(fixedScale);
        config.setNumberNullScaleType("DECIMAL");
        config.setNumberZeroScaleType("INTEGER");

        OracleNumberHandling numberHandling = buildNumberHandling(Decimals.MAX_PRECISION, OracleConfig.UNDEFINED_SCALE, config);
        assertEquals(numberHandling.getMapToType(), JDBCType.DECIMAL);
        DecimalType expectedType = createDecimalType(Decimals.MAX_PRECISION, fixedScale);
        ReadMapping readExpected = OracleReadMappings.roundDecimalReadMapping(expectedType, config.getNumberDecimalRoundMode());
        ReadMapping readCompare = numberHandling.getReadMapping();
        TestOracleReadMappings.assertReadMappingEquals(readExpected, readCompare);

        // Ensure an undefined scale is treated as a default scale
        DecimalType decType = (DecimalType) readCompare.getType();
        assertEquals(decType.getScale(), fixedScale);
        assertEquals(decType.getPrecision(), Decimals.MAX_PRECISION);
    }

    @Test void testDecimalNullScaleAsDecimalWithRatio()
    {
        float ratioScale = 0.3f;
        int expectedScale = (int) ((float) Decimals.MAX_PRECISION * ratioScale);

        OracleConfig config = buildConfig();
        config.setNumberDecimalDefaultScaleFixed(OracleConfig.UNDEFINED_SCALE);
        config.setNumberDecimalDefaultScaleRatio(ratioScale);
        config.setNumberNullScaleType("DECIMAL");
        config.setNumberZeroScaleType("INTEGER");

        OracleNumberHandling numberHandling = buildNumberHandling(Decimals.MAX_PRECISION, OracleConfig.UNDEFINED_SCALE, config);
        assertEquals(numberHandling.getMapToType(), JDBCType.DECIMAL);
        DecimalType expectedType = createDecimalType(Decimals.MAX_PRECISION, expectedScale);
        // when scale is undefined, the oracle round read mapping will be used.
        ReadMapping readExpected = OracleReadMappings.roundDecimalReadMapping(expectedType, config.getNumberDecimalRoundMode());
        ReadMapping readCompare = numberHandling.getReadMapping();
        TestOracleReadMappings.assertReadMappingEquals(readExpected, readCompare);

        // Ensure an undefined scale is treated as a default scale
        DecimalType decType = (DecimalType) readCompare.getType();
        assertEquals(decType.getScale(), expectedScale);
        assertEquals(decType.getPrecision(), Decimals.MAX_PRECISION);
    }

    private OracleNumberHandling buildNumberHandling(int precision, int scale, OracleConfig config)
    {
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.NUMERIC, precision, scale);
        OracleNumberHandling numberHandling = new OracleNumberHandling(typeHandle, config);
        return numberHandling;
    }

    private OracleConfig buildConfig() {
        OracleConfig config = new OracleConfig()
                .setNumberExceedsLimitsMode("ROUND")
                .setNumberTypeDefault("DECIMAL")
                .setNumberZeroScaleType("INTEGER")
                .setNumberNullScaleType("DECIMAL")
                .setNumberDecimalRoundMode("UP")
                .setNumberDecimalDefaultScaleFixed(8);
        return config;
    }

}