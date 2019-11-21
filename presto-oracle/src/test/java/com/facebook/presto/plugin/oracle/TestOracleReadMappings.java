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
import com.facebook.presto.spi.type.*;
import io.airlift.slice.Slice;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static java.lang.String.format;
import static org.testng.Assert.*;

public class TestOracleReadMappings
{
    private DecimalType dec2Scale = createDecimalType(Decimals.MAX_PRECISION, 2);
    private DecimalType dec8Scale = createDecimalType(Decimals.MAX_PRECISION, 8);
    private DecimalType dec15Scale = createDecimalType(Decimals.MAX_PRECISION, 15);

    @Test void testType()
    {
        ReadMapping read = OracleReadMappings.roundDecimalReadMapping(dec2Scale, RoundingMode.UP);
        assertEquals(read.getType().getJavaType(), Slice.class);
    }

    @Test void testRound()
    {
        ReadMapping roundDecimal2 = OracleReadMappings.roundDecimalReadMapping(dec2Scale, RoundingMode.UP);
        ReadMapping roundDecimal8 = OracleReadMappings.roundDecimalReadMapping(dec8Scale, RoundingMode.UP);
        ReadMapping roundDecimal15 = OracleReadMappings.roundDecimalReadMapping(dec15Scale, RoundingMode.UP);

        assertSliceReadMappingRoundTrip(new BigDecimal("1.00"), new BigDecimal("1"), roundDecimal2, dec2Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("1.00"), new BigDecimal("1.0"), roundDecimal2, dec2Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("1.20"), new BigDecimal("1.199"), roundDecimal2, dec2Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("0.10"), new BigDecimal("0.099"), roundDecimal2, dec2Scale);

        assertSliceReadMappingRoundTrip(new BigDecimal("1.12345679"), new BigDecimal("1.123456789"), roundDecimal8, dec8Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("0.00000002"), new BigDecimal("0.000000019"), roundDecimal8, dec8Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("100.00000001"), new BigDecimal("100.000000009"), roundDecimal8, dec8Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("500.00000000"), new BigDecimal("500"), roundDecimal8, dec8Scale);

        assertSliceReadMappingRoundTrip(new BigDecimal("0.68" + repeat("0", 13)), new BigDecimal("0.680"), roundDecimal15, dec15Scale);
        assertSliceReadMappingRoundTrip(new BigDecimal("98765.1" + repeat("0", 14)), new BigDecimal("98765.10"), roundDecimal15, dec15Scale);
    }

    private void assertSliceReadMappingRoundTrip(BigDecimal correctValue, BigDecimal testInput, ReadMapping readMapping, DecimalType decimalType) {
        try {
            int scale = decimalType.getScale();
            ResultSet rs = getMockResultSet(0, testInput);
            SliceReadFunction readFn = (SliceReadFunction) readMapping.getReadFunction();
            Slice testSlice = readFn.readSlice(rs, 0);
            Slice correctSlice = encodeUnscaledValue(correctValue.unscaledValue());

            String testVal = Decimals.toString(testSlice, scale);
            String correctVal = Decimals.toString(correctSlice, scale);
            if(!testSlice.equals(correctSlice)) {
                fail(format("Slice values do not match => expected: %s, sliceOutput: %s", correctVal, testVal));
            }
            if(!testVal.equals(correctVal)) {
                fail(format("Slice (text) values do not match => expected: %s, sliceOutput: %s", correctVal, testVal));
            }
            BigDecimal decVal = toBigDecimal(testSlice, scale);
            BigDecimal decCorrect = toBigDecimal(correctSlice, scale);
            if(!decVal.equals(decCorrect)) {
                fail(format("Slice (decimal) values do not match => expected: %s, sliceOutput: %s", decCorrect, decVal));
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static ResultSet getMockResultSet(int columnIndex, Object value) {
        ResultSet resultSetMock = Mockito.mock(ResultSet.class);
        try {
            if(value instanceof BigDecimal) {
                Mockito.when(resultSetMock.getBigDecimal(columnIndex)).thenReturn((BigDecimal) value);
            } else if (value instanceof Double) {
                Mockito.when(resultSetMock.getDouble(columnIndex)).thenReturn((Double) value);
            } else if (value instanceof Long) {
                Mockito.when(resultSetMock.getLong(columnIndex)).thenReturn((Long) value);
            } else if (value instanceof Boolean) {
                Mockito.when(resultSetMock.getBoolean(columnIndex)).thenReturn((Boolean) value);
            } else if (value instanceof Short) {
                Mockito.when(resultSetMock.getShort(columnIndex)).thenReturn((Short) value);
            } else if (value instanceof Integer) {
                Mockito.when(resultSetMock.getInt(columnIndex)).thenReturn((Integer) value);
            }
            // provide a getString() version always
            Mockito.when(resultSetMock.getString(columnIndex)).thenReturn((String) value.toString());
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return resultSetMock;
    }

    private static BigDecimal toBigDecimal(Slice valueSlice, int scale)
    {
        return new BigDecimal(unscaledDecimalToBigInteger(valueSlice), scale);
    }

    public static void assertPrestoTypeEquals(Type t1, Type t2)
    {
        if(!t1.getClass().equals(t2.getClass())) {
            fail(format("Presto data-types are not identical expected [%s] but found [%s]", t1.getClass(), t2.getClass()));
        }
        if(t1 instanceof DecimalType) {
            DecimalType t1Dec = (DecimalType) t1;
            DecimalType t2Dec = (DecimalType) t2;
            if(t1Dec.getPrecision() != t2Dec.getPrecision()
                    || t1Dec.getScale() != t2Dec.getScale()) {
                fail(format("Presto DecimalTypes not identical expected [%s] but found [%s]", t1Dec, t2Dec));
            }
        }
    }

    private static String repeat(String value, int times) {
        return String.format("%0" + times  + "d", 0).replace("0", value);
    }

    public static void assertPrestoReadFunctionEquals(ReadFunction r1, ReadFunction r2, Type prestoType)
    {
        if(!r1.getClass().equals(r2.getClass())) {
            fail(format("ReadFunctions (class types) are not identical, expected [%s] but found [%s]", r1, r2, r1.getClass(), r2.getClass()));
        }

        if(!r1.getJavaType().equals(r2.getJavaType())) {
            fail(format("ReadFunctions 1:%s - 2:%s (return types) are not identical, expected [%s] but found [%s]", r1, r2, r1.getJavaType(), r1.getJavaType()));
        }

        if (r1.getJavaType() == boolean.class) {
            BooleanReadFunction boolR1 = (BooleanReadFunction) r1;
            BooleanReadFunction boolR2 = (BooleanReadFunction) r2;
            assertEquals(boolR1.getJavaType(), boolR2.getJavaType());
        }
        else if (r1.getJavaType() == double.class) {
            DoubleReadFunction dblR1 = (DoubleReadFunction) r1;
            DoubleReadFunction dblR2 = (DoubleReadFunction) r2;
            assertEquals(dblR1.getJavaType(), dblR2.getJavaType());
        }
        else if (r1.getJavaType() == long.class) {
            LongReadFunction longR1 = (LongReadFunction) r1;
            LongReadFunction longR2 = (LongReadFunction) r2;
            assertEquals(longR1.getJavaType(), longR2.getJavaType());
        }
        else if (r1.getJavaType() == Slice.class) {
            SliceReadFunction sliceR1 = (SliceReadFunction) r1;
            SliceReadFunction sliceR2 = (SliceReadFunction) r2;
            assertEquals(sliceR1.getJavaType(), sliceR2.getJavaType());
            Object value;

            if(prestoType instanceof DecimalType) {
                DecimalType decType = (DecimalType) prestoType;
                // create a value that will fit the given decimal type, and one that won't
                String zeros0 = repeat("9", decType.getScale());
                // this won't fit the decimal type, thus rounding will be tested (if possible)
                String zeros1 = zeros0 + "9";
                value = new BigDecimal("123." + zeros1);

                // If the value needs rounding, reduce the decimal scale to a value that will fit prestoType
                // this portion of the logic is specific to Oracle (we support rounding)
                try { sliceR1.readSlice(getMockResultSet(0, value), 0); }
                catch (SQLException ex) { throw new RuntimeException(ex); }
                catch (ArithmeticException ex) {
                    if (!ex.getMessage().toLowerCase().contains("rounding necessary")) {
                        throw ex;
                    }
                    value = new BigDecimal("123." + zeros0);
                }

            } else if (prestoType instanceof CharType) {
                value = new Character('a');
            } else if (prestoType instanceof VarcharType) {
                value = new String("a string");
            } else if (prestoType instanceof VarbinaryType) {
                value = new Byte("a binary string");
            } else {
                throw new RuntimeException("unsupported Slice type: " + prestoType.toString());
            }

            ResultSet rs = getMockResultSet(0, value);
            Slice s1;
            Slice s2;
            try {
                s1 = sliceR1.readSlice(rs, 0);
                s2 = sliceR2.readSlice(rs, 0);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

            assertEquals(s1, s2);
        }
    }

    public static void assertReadMappingEquals(ReadMapping r1, ReadMapping r2)
    {
        assertPrestoTypeEquals(r1.getType(), r2.getType());
        assertPrestoReadFunctionEquals(r1.getReadFunction(), r2.getReadFunction(), r1.getType());
    }

    public static void assertReadMappingNotEquals(ReadMapping r1, ReadMapping r2)
    {
        assertThrows(AssertionError.class, () -> TestOracleReadMappings.assertReadMappingEquals(r1, r2));
    }
}