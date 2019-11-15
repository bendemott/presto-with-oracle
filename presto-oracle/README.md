# Presto Oracle
A Presto Oracle Driver

# Configuration
Configuration properties are inherited from BaseJdbcConfig, I've included all options here.
```properties
connector.name=oracle
connection-url=jdbc:oracle:thin://ip:port/database
connection-user=myuser
connection-password=****
oracle.auto-reconnect=true
oracle.connection-timeout=10
oracle.max-reconnects=3
```

Rounding Mode can be one of ``UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN, UNNECESSARY``
If set to ``UNNECESSARY`` no rounding will be performed.

# Jar Dependency
An oracle JDBC driver must be provided in the plugin directory.
By default the class `oracle.jdbc.OracleDriver` is checked to ensure the Jar has been loaded correctly.
You can override which class is used for the class check by specifying the java property: `com.facebook.presto.plugin.oracle.ImportTest`

# Issues
- Predicate Pushdown not working?
- Handling of Rounding / Out of Bounds numeric

# Planned Features
- Oracle Synonym Support
- Oracle "NUMBER" support
- Connection Pooling Support
- Histogram / Analysis support
- Multi-Threading

# Notes
```
OraclePlugin is the "plugin" class provided to the Presto SPI and is where the plugin name "oracle" is specified
OracleConnectionFactory is the entry point for accessing the oracle libraries during plugin loading.
  it also checks to ensure that the Oracle Plugin has been loaded and logs the version of the jar loaded.
OracleClient is where the OracleDriver is referenced, and calls into DriverConnectionFactory
DriverConnectionFactory is where the connection is actually made
```

# Config

```properties
"connector.name": "oracle",
"connection-url": "jdbc:oracle:thin:@usmliu168.arrow.com:1522:utyeb19",
"connection-user": "a88688",
"connection-password": "welcome1",
"unsupported-type.handling-strategy": "CONVERT_TO_VARCHAR",
"oracle.synonyms.enabled": false
"oracle.number.exceeds-limits": "CONVERT_TO_VARCHAR", "ROUND" to enable rounding mode
"oracle.number.default-type": "DECIMAL",
"oracle.number.precision.zero-scale-type": "INTEGER",
"oracle.number.precision.null-scale-type": "DECIMAL",
"oracle.number.precision.as-integer": "0:-127",
"oracle.number.precision.as-double": "",
"oracle.number.precision.as-decimal": "15:null",
"oracle.number.decimal.scale-map": "null:null=38:12",  (map precision to desired scale)
"oracle.number.decimal.default-scale.absolute": 0,
"oracle.number.decimal.default-scale.ratio": 0.3,
"oracle.number.decimal.round-mode": "HALF_EVEN",
```