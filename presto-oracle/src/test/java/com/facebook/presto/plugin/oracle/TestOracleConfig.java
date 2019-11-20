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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class TestOracleConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(OracleConfig.class)
                .setSynonymsEnabled(false)
                .setUnsupportedTypeStrategy("IGNORE")
                .setAutoReconnect(true)
                .setMaxReconnects(3)
                .setConnectionTimeout(new Duration(10, TimeUnit.SECONDS))
                .setNumberExceedsLimitsMode("ROUND")
                .setNumberTypeDefault("DECIMAL")
                .setNumberZeroScaleType("")
                .setNumberNullScaleType("")
                .setNumberAsIntegerTypes("")
                .setNumberAsDoubleTypes("")
                .setNumberAsDecimalTypes("")
                .setNumberDecimalRoundMode("HALF_EVEN")
                .setNumberDecimalDefaultScaleFixed(OracleConfig.UNDEFINED_SCALE)
                .setNumberDecimalDefaultScaleRatio(OracleConfig.UNDEFINED_SCALE)
                .setNumberDecimalPrecisionMap(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("unsupported-type.handling-strategy", "IGNORE")
                .put("oracle.synonyms.enabled", "false")
                .put("oracle.connection-timeout", "10s")
                .put("oracle.auto-reconnect", "true")
                .put("oracle.max-reconnects", "3")
                .put("oracle.number.exceeds-limits", "ROUND")
                .put("oracle.number.type.as-integer", "")
                .put("oracle.number.type.as-double", "")
                .put("oracle.number.type.as-decimal", "")
                .put("oracle.number.type.default", "DECIMAL")
                .put("oracle.number.type.zero-scale-type", "INTEGER")
                .put("oracle.number.type.null-scale-type", "DECIMAL")
                .put("oracle.number.decimal.round-mode", "HALF_EVEN")
                .put("oracle.number.decimal.default-scale.fixed", String.valueOf(OracleConfig.UNDEFINED_SCALE))
                .put("oracle.number.decimal.default-scale.ratio", String.valueOf(OracleConfig.UNDEFINED_SCALE))
                .put("oracle.number.decimal.precision-map", "")
                .build();

        OracleConfig expected = new OracleConfig();

        //ConfigAssertions.assertFullMapping(properties, expected);
    }
}