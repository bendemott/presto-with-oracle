package com.facebook.presto.plugin.oracle;

import com.facebook.presto.spi.type.Decimals;
import org.testng.annotations.Test;

import java.sql.Types;

import static org.testng.Assert.assertEquals;

public class TestOracleJdbcTypeHandle {
    @Test void testAssignment()
    {
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, 4, 2);
        assertEquals(typeHandle.getColumnSize(), 4);
        assertEquals(typeHandle.getDecimalDigits(), 2);
        assertEquals(typeHandle.getPrecision(), 4);
        assertEquals(typeHandle.getScale(), 2);
    }

    @Test void testAssignmentLimitsExceededPassThrough()
    {
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, Decimals.MAX_PRECISION + 1, Decimals.MAX_PRECISION + 2);
        assertEquals(typeHandle.getPrecision(), Decimals.MAX_PRECISION + 1);
        assertEquals(typeHandle.getScale(), Decimals.MAX_PRECISION + 2);
    }

    @Test void testPrecisionLimit()
    {
        int precision = Decimals.MAX_PRECISION + 1;
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, precision, 2);
        assertEquals(typeHandle.isPrecisionUndefined(), false);
        assertEquals(typeHandle.isPrecisionLimitExceeded(), true);
        assertEquals(typeHandle.getPrecision(), precision);
    }

    @Test void testPrecisionUndefined()
    {
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, 0, OracleJdbcTypeHandle.UNDEFINED_SCALE);
        assertEquals(typeHandle.isPrecisionUndefined(), true);
        assertEquals(typeHandle.isPrecisionLimitExceeded(), false);
        assertEquals(typeHandle.getPrecision(), 0);
    }


    @Test void testScaleLimit()
    {
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, 4, Decimals.MAX_PRECISION + 1);
        assertEquals(typeHandle.isScaleLimitExceeded(), true);
        assertEquals(typeHandle.isScaleUndefined(), false);
    }

    @Test void testScaleUndefined()
    {
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, 4, OracleJdbcTypeHandle.UNDEFINED_SCALE);
        assertEquals(typeHandle.isScaleUndefined(), true);
        assertEquals(typeHandle.isScaleLimitExceeded(), false);
        assertEquals(typeHandle.getScale(), OracleJdbcTypeHandle.UNDEFINED_SCALE);
    }

    @Test void testScaleUndefinedExplicit()
    {
        // The constant -127 is hard coded into the Oracle JDBC Driver, and should never change, nor should our constant
        OracleJdbcTypeHandle typeHandle = new OracleJdbcTypeHandle(Types.DECIMAL, 4, -127);
        assertEquals(typeHandle.isScaleUndefined(), true);
        assertEquals(typeHandle.isScaleLimitExceeded(), false);
        assertEquals(typeHandle.getScale(), -127);
    }

    @Test void testNegativeScaleToPositive()
    {
        OracleJdbcTypeHandle typeHandle1 = new OracleJdbcTypeHandle(Types.DECIMAL, 0, -10);
        assertEquals(typeHandle1.getScale(), 0);
        assertEquals(typeHandle1.getPrecision(), 10);
        assertEquals(typeHandle1.isPrecisionLimitExceeded(), false);
        assertEquals(typeHandle1.isPrecisionUndefined(), false);
    }
    @Test void testNegativeScaleAddedToPositive()
    {
        OracleJdbcTypeHandle typeHandle2 = new OracleJdbcTypeHandle(Types.DECIMAL, 4, -10);
        assertEquals(typeHandle2.getScale(), 0);
        assertEquals(typeHandle2.getPrecision(), 14);
        assertEquals(typeHandle2.isPrecisionLimitExceeded(), false);
        assertEquals(typeHandle2.isPrecisionUndefined(), false);
    }

    @Test void testNegativeScaleExceedLimit()
    {
        int negMax = Decimals.MAX_PRECISION * -1;
        OracleJdbcTypeHandle typeHandle3 = new OracleJdbcTypeHandle(Types.DECIMAL, 1, negMax);
        assertEquals(typeHandle3.getScale(), 0);
        assertEquals(typeHandle3.getPrecision(), 39);
        assertEquals(typeHandle3.isPrecisionLimitExceeded(), true);
        assertEquals(typeHandle3.isPrecisionUndefined(), false);
    }
}
