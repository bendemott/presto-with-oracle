# Presto Oracle
A Presto Oracle Driver that offers commercial grade features, performance and customization.

Currently targets PrestoDB, will be ported to PrestoSQL (WIP)

## Author
- Ben DeMott (Arrow)
- ben.demott@gmail.com

## Features
- (Optional) Oracle Synonym support via `oracle.synonyms.enabled=true`
- Correct handling of Oracle `NUMBER` type, including many special options
- Correct handling of Oracle `DATE` type

## Jar Dependency
An oracle JDBC driver must be provided in the plugin directory.
By default the class `oracle.jdbc.OracleDriver` is checked to ensure the Jar has been loaded correctly.
You can override which class is used for the class check by specifying the java property: `com.facebook.presto.plugin.oracle.ImportTest`

## Tests
- Decent test coverage for complex logic
- Plan to improve overall test coverage
- End-to-End CI (Oracle Docker) tests are WIP

## Issues
- user SYNONYMS work, but synonyms through a grant do not
- BLOB support `failed: Invalid column type: getString/getNString not implemented for class oracle.jdbc.driver.T4CBlobAccessor`
- INSERT support `com.facebook.presto.spi.PrestoException: ORA-00972: identifier is too long` 
- UNBOUNDED VARCHAR

## Features
This driver features several **Oracle** specific enhancements and compatibilities to make integration more seamless.

### Future
- Connection Pooling Support
- Histogram / Analysis support
- Multi-Threading


### Driver Version / Logging
During startup the JDBC driver provided on the class path will be loaded and validated.
The JDBC driver version will be printed to the screen based on the SHA-1 hash of the driver used.

### Synonyms
Synonyms can be enabled via `oracle.synonyms.enabled`, they are disabled by default. Enabling synonyms will have a small performance impact.

### Type Handling

#### NUMBER
In oracle a `NUMBER` can be defined in multiple ways, or left undefined.
The `NUMBER` type in Oracle can also be used as an `INTEGER` type as well.
For this reason the driver provides a myriad of options on how you want to treat this special type.

#### Defining Precision and Scale for `NUMBER` in **Oracle**
*Where the `precision` is the total number of digits and `scale` is the number of digits right or left (negative scale) of the decimal point.*

```
                Resulting  Resulting  Precision
Specification   Precision  Scale      Check      Comment
―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
NUMBER          NULL       NULL       NO         values are stored 'as given'
NUMBER(P, S)    P          S          YES        Error code: ORA-01438
NUMBER(P)       P          0          YES        Error code: ORA-01438
NUMBER(*, S)    38         S          NO
```

#### DATE
`DATE` types in Oracle can be DATETIME, so all Date types are converted to `DATETIME`

# Config
This section explains configuration options, and provides some default configurations depending on your use case.

## Configuration Settings Summary
| Value                                       | Options/Example                                                                    | Default     | Description                                                           |
| ------------------------------------------- | ---------------------------------------------------------------------------------- | ----------- | --------------------------------------------------------------------- |
| `unsupported-type.handling-strategy`        | `FAIL`, `IGNORE`, `VARCHAR`                                                        | `IGNORE`    | Action to take when unsupported or custom oracle types are encounered 
| `oracle.auto-reconnect`                     | `true`, `false`                                                                    | `true`      | Controls if the oracle jdbc driver reconnects automatically           
| `oracle.max-reconnects`                     | `1`, `10`                                                                          | `3`         | Max number of times the jdbc driver will reconnect if enabled                                                                      
| `oracle.connection-timeout`                 | `1s`, `1m`, `30s`                                                                  | `10s`       | Connection timeout, `1 second` is specified using `1s`                
| `oracle.synonyms.enabled`                   | `true`, `false`                                                                    | `false`     | Determines if Synonyms can be used in queries                      
| `oracle.number.type.default`                | `DECIMAL`, `DOUBLE`, `INTEGER`, `VARCHAR`                                          | `DECIMAL`   | Default type to convert Oracle `NUMBER` types to. Overridden by: `oracle.number.type.as-double`, `oracle.number.type.as-integer`, `oracle.number.type.as-varchar`, `oracle.number.type.zero-scale-type`, `oracle.number.type.null-scale-type`                                             
| `oracle.number.exceeds-limits`              | `ROUND`, `VARCHAR`, `IGNORE`, `FAIL`                                               | `ROUND`     | Determines what action to take when a `NUMBER` column type exceeds the limits of Presto
| `oracle.number.decimal.round-mode`          | `CEILING`, `DOWN`, `FLOOR`,`HALF_EVEN`, `HALF_DOWN`, `HALF_UP`, `UP`, `UNNECESSARY`| `HALF_EVEN` | When a NUMBER is treated as a DECIMAL it will be rounded using this method
| `oracle.number.decimal.default-scale.fixed` | `1`, `2`, `35`                                                                     |             | When a `NUMBER` is treated as a `DECIMAL` its scale will be set to this value
| `oracle.number.decimal.default-scale.ratio` | `0.1`, `0.3`, `0.5`                                                                |             | When a `NUMBER` is treated as a `DECIMAL` its scale will be set as a fraction of the columns precision (or 38) if undefined
| `oracle.number.decimal.precision-map`       | `null:null=38:12`                                                                  |             | Matches explicit `precision:scale` pairs and maps them to presto `precision:scale` pairs.
| `oracle.number.double.default-scale.fixed`  | `1`, `4`, `8`                                                                      |             | When a `NUMBER` is treated as a `DOUBLE` it will be rounded to this scale
| `oracle.number.double.round-mode`           | `CEILING`, `DOWN`, `FLOOR`,`HALF_EVEN`, `HALF_DOWN`, `HALF_UP`, `UP`, `UNNECESSARY`| `HALF_EVEN` | When a `NUMBER` is treated as a `DOUBLE` it will be rounded using this method if needed
| `oracle.number.type.zero-scale-type`        | `DECIMAL`, `DOUBLE`, `INTEGER`, `VARCHAR`                                          | `INTEGER`   | When a `NUMBER` scale is zero it will be treated as this type
| `oracle.number.type.null-scale-type`        | `DECIMAL`, `DOUBLE`, `INTEGER`, `VARCHAR`                                          | `DECIMAL`   | When a `NUMBER` scale is undefined (`null`) it will be mapped to this type
| `oracle.number.type.as-integer`             | `10:6`                                                                             |             | Controls what (`precision:scale`) pairs will be treated as INTEGER
| `oracle.number.type.as-double`              | `10:6`                                                                             |             | Controls what (`precision:scale`) pairs will be treated as DOUBLE
| `oracle.number.type.as-decimal`             | `10:6`                                                                             |             | Controls what (`precision:scale`) pairs will be treated as DECIMAL
| `oracle.number.type.as-varchar`             | `10:6`                                                                             |             | Controls what (`precision:scale`) pairs will be treated as VARCHAR                                                                      

## Example Configs
### Basic config options
Configuration properties are inherited from BaseJdbcConfig
```properties
connector.name=oracle
connection-url=jdbc:oracle:thin://ip:port/database
connection-user=myuser
connection-password=****
oracle.auto-reconnect=true
oracle.connection-timeout=10
oracle.max-reconnects=3
```

### Default NUMBER as `DECIMAL` with Rounding
In this configuration all `NUMBER` types will be converted to `DECIMAL`

```properties
connector.name=oracle
connection-url=jdbc=oracle=thin=@example.domain.com=1522=sid
connection-user=username
connection-password=password
unsupported-type.handling-strategy=VARCHAR
oracle.synonyms.enabled=true
oracle.number.exceeds-limits=ROUND
oracle.number.default-type=DECIMAL
oracle.number.type.zero-scale-type=INTEGER
oracle.number.type.null-scale-type=DECIMAL
oracle.number.decimal.default-scale.ratio=0.3
oracle.number.decimal.round-mode=HALF_EVEN
```

### Default NUMBER as `VARCHAR`
In this configuration we treat all Oracle `NUMBER` types as strings to preserve their accuracy exactly.

When a NUMBER is encountered it will be converted 

Except for `NUMBER` columns with a scale defined as zero (`0`) are converted to `INTEGER`.
```properties
connector.name=oracle
connection-url=jdbc=oracle=thin=@example.domain.com=1522=sid
connection-user=username
connection-password=password
unsupported-type.handling-strategy=VARCHAR
oracle.synonyms.enabled=true
oracle.number.default-type=VARCHAR
oracle.number.type.zero-scale-type=INTEGER
```

## Configuration Properties

### `unsupported-type.handling-strategy`
- values: `IGNORE`, `FAIL`, `VARCHAR`
- description: Determines how unsupported or custom types within Oracle are handled. Columns that are unsupported will
               be ignored and excluded from the query result by setting `IGNORE`.
               An exception will be raised when these columns are selected when `FAIL` is set.
               And finally, `VARCHAR` will convert unsupported columns to text.
- default: `IGNORE`

### `oracle.synonyms.enabled`
- **values:** `true` | `false`
- **description:** When enabled allows queries to use Oracle Synonyms. Synonyms in oracle are table and schema *aliases* that 
  map to underlying tables.
- **default:** `false`

### `oracle.number.type.default`
- **values:** `DECIMAL`, `DOUBLE`, `INTEGER`, `VARCHAR`
- **description:** Determines the data type Oracle `NUMBER` will be converted to.
                   Presto doesn't feature a variadic type or `NUMBER` type like Oracle for performance reasons.

### `oracle.number.exceeds-limits`
- **values:** `IGNORE`, `FAIL`, `VARCHAR`, `ROUND`
- **description:** Determines the behavior of `NUMBER` types that have undefined scale/precision or exceed the limits
                   of Presto. 
                   Behaves similarly to `unsupported-type.handling-strategy`, except with the additional option
                   of `ROUND` which is explained in the table below.
                   
                   
#### `ROUND`
Note that even when rounding is enabled, values that simply cannot fit into the Presto 128bit `DECIMAL` type will still
throw exceptions.

Rounding is triggered under 2 conditions by default
1. `NUMBER(null,null)` - column type precision or scale are not set.
2. `NUMBER(100,50)` - column type precision/scale exceed the limits of Presto's `DECIMAL` type.

When `ROUND` is used these other options must be set.
       
| AFFECTS  | TYPE    | VALUE                                       | Comments                                    |
|----------|---------|---------------------------------------------|---------------------------------------------|
| ROUNDING | DECIMAL | `oracle.number.decimal.round-mode`          | Used when NUMBER is mapped to `DECIMAL`     |
| ROUNDING | DOUBLE  | `oracle.number.double.round-mode`           | Used when NUMBER is mapped to `DOUBLE`      |
| SCALE    | DECIMAL | `oracle.number.decimal.default-scale.fixed` | Use only one `number.decimal.default-scale` |
| SCALE    | DECIMAL | `oracle.number.decimal.default-scale.ratio` | Use only one `number.decimal.default-scale` |
| SCALE    | DOUBLE  | `oracle.number.double.default-scale.fixed`  | Used when NUMBER is mapped to `DOUBLE`      |


### `oracle.number.decimal.round-mode`
- **values:** `CEILING`, `DOWN`, `FLOOR`, `HALF_EVEN`, `HALF_DOWN`, `HALF_UP`, `UP`, `UNNECESSARY`
- **description:** When `oracle.number.exceeds-limits` is set to `ROUND` - this value determines how `DECIMAL`
                   values are rounded.
                   
### `oracle.number.decimal.default-scale.fixed`
- **values:** `integer`
- **description:** When `oracle.number.type.default` is set to `DECIMAL` and a columns scale is undefined; this setting
                   applies a default fixed scale. The table below explains how different `NUMBER` type definitions would 
                   be handled.
- **conflicts-with:** `oracle.number.decimal.default-scale.ratio`

| Oracle Definition   | fixed-scale | Presto Definition | Comment                                                     |
|---------------------|:-----------:|-------------------|-------------------------------------------------------------|
| `NUMBER(10,2)`      |      8      | `DECIMAL(10,2)`   | scale is set in Oracle, no action is taken                  |
| `NUMBER(20,null)`   |      8      | `DECIMAL(20,8)`   | scale is set to 8                                           |
| `NUMBER(null,null)` |      2      | `DECIMAL(38,2)`   | precision is undefined, set to max (38)                     |
| `NUMBER(null,40)`   |      10     | `DECIMAL(38,10)`  | scale exceeds limits, set to 10                             |
| `NUMBER(80,null)`   |      10     | `DECIMAL(38,10)`  | precision exceeds limits, set to max (38)                   |
| `NUMBER(*,-20)`     |      10     | `DECIMAL(20,0)`   | negative scale, is positive precision, scale is set to zero |
                   
### `oracle.number.decimal.default-scale.ratio`
- **values:** `integer`
- **description:** When `oracle.number.type.default` is set to `DECIMAL` and a columns scale is undefined; this setting
                  applies a default scale based on a ratio to the types precision (if defined).
                  If the precision is not defined the MAX_PRECISION will be used.
                  The table below explains how different `NUMBER` type definitions would be handled.
- **conflicts-with:** `oracle.number.decimal.default-scale.fixed`

| Oracle Definition   | ratio-scale | Presto Definition | Comment                                                      |
|---------------------|:-----------:|-------------------|--------------------------------------------------------------|
| `NUMBER(10,2)`      |     0.4     | `DECIMAL(10,2)`   | scale is set in Oracle, no action is taken                   |
| `NUMBER(20,null)`   |     0.4     | `DECIMAL(20,8)`   | scale is set to 8 `(20 * 0.4) = 8`                           |
| `NUMBER(null,null)` |     0.4     | `DECIMAL(38,15)`  | precision defaults to 38... `(38 * 0.4) = 15`                |
| `NUMBER(null,40)`   |     0.4     | `DECIMAL(38,15)`  | precision defaulted, scaled out-of-bounds, `(38 * 0.4) = 15` |
| `NUMBER(80,null)`   |     0.2     | `DECIMAL(38,8)`   | precision exceeds limits, set to max (38), `(38 * 0.2) = 8`  |
| `NUMBER(*,-20)`     |     0.2     | `DECIMAL(20,0)`   | negative scale, is positive precision, scale is set to zero  |

### `oracle.number.type.zero-scale-type`
- **values:** `DECIMAL`, `INTEGER`, `DOUBLE`, `VARCHAR`
- **description:** When a `NUMBER` type is encountered with a scale explicitly set to zero (`0`), treat this column
                   as the given data-type.  Default is `INTEGER` - a NUMBER with no decimal digits can be thought of 
                   as an INTEGER under most circumstances.
- **overrides:** `oracle.number.type.default`

### `oracle.number.type.null-scale-type`
- **values:** `DECIMAL`, `INTEGER`, `DOUBLE`, `VARCHAR`
- **description:** When a `NUMBER` type is encountered with a scale that is undefined, treat it as this data-type.
                   Defaults to `DECIMAL`. 
                   Useful for in some circumstances where an undefined scale can be handled by a `DOUBLE`.
                   Or you want to preserve the accuracy of undefined types so you treat them as `VARCHAR`.
- **overrides:** `oracle.number.type.default`