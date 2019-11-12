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

import com.facebook.presto.plugin.oracle.UnsupportedTypeHandling;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import java.math.RoundingMode;
import javax.validation.constraints.Min;
import java.util.concurrent.TimeUnit;

public class OracleConfig
{
    public static int UNDEFINED_SCALE = -127;
    private UnsupportedTypeHandling typeStrategy = UnsupportedTypeHandling.IGNORE;
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean synonymsEnabled = false;
    private int numberScale = UNDEFINED_SCALE;
    private boolean undefinedScaleAsInteger = false;
    private RoundingMode roundingMode = RoundingMode.UNNECESSARY;

    // oracle.synonyms.enabled
    // unsupported-type.handling-strategy {FAIL | IGNORE | CONVERT_TO_VARCHAR}

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

    public UnsupportedTypeHandling getUnsupportedTypeStrategy()
    {
        return typeStrategy;
    }

    @Config("unsupported-type.handling-strategy")
    public OracleConfig setUnsupportedTypeStrategy(String typeStrategy)
    {
        this.typeStrategy = UnsupportedTypeHandling.valueOf(typeStrategy);
        return this;
    }

    public RoundingMode getRoundingMode()
    {
        return roundingMode;
    }

    @Config("oracle.number.rounding-mode")
    public OracleConfig setRoundingMode(String roundingMode)
    {
        this.roundingMode = RoundingMode.valueOf(roundingMode);
        return this;
    }

    public boolean isUndefinedScaleInteger()
    {
        return undefinedScaleAsInteger;
    }

    @Config("oracle.number.zero-scale-as-integer")
    public OracleConfig setUndefinedScaleInteger(boolean undefinedScaleAsInteger)
    {
        this.undefinedScaleAsInteger = undefinedScaleAsInteger;
        return this;
    }

    public int getDefaultNumberScale()
    {
        return numberScale;
    }

    @Config("oracle.number.default-scale")
    public OracleConfig setDefaultNumberScale(int numberScale)
    {
        this.numberScale = numberScale;
        return this;
    }

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
}
