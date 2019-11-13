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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import java.io.FileInputStream;
import java.security.MessageDigest;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.hash.HashCode;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class OracleConnectorFactory
        implements ConnectorFactory
{
    private static final Logger LOG = Logger.get(OracleConnectorFactory.class);
    private final String name;
    private final String ORACLE_IMPORT_TEST = "oracle.jdbc.OracleDriver";
    // ORACLE_IMPORT_PROP - use to override ORACLE_IMPORT_TEST value
    private final String ORACLE_IMPORT_PROP = "com.facebook.presto.plugin.oracle.ImportTest";
    private final Module module;
    private final ClassLoader classLoader;


    public OracleConnectorFactory(String name, Module module, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    /**
     * Returns the name of the plugin "oracle"
     */
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new JdbcHandleResolver(); // why override this?
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        requireOracleDriver();

        // based off of JdbcConnectorFactory
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {

            Bootstrap app = new Bootstrap(
                    binder -> {
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                    },
                    new OracleModule(catalogName),
                    module);

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(OracleConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * The Oracle Driver is provided for us via a JAR present in the classpath.
     * If it's not present raise a useful error.
     * If it is present, inform the user which JAR they've provided via its SHA1 hash.
     */
    private void requireOracleDriver() {
        String overrideImport = System.getProperty(ORACLE_IMPORT_PROP);
        String oracleImport = ORACLE_IMPORT_TEST;

        if (overrideImport != null && !overrideImport.isEmpty()) {
            oracleImport = overrideImport;
        }

        Map<String,String> oracleDriverHashes = new HashMap<>();
        oracleDriverHashes.put("a483a046eee2f404d864a6ff5b09dc0e1be3fe6c", "11.2.0.4 (11g Release 2)"); // JDK 6-8
        oracleDriverHashes.put("5543067223760fc2277fe3f603d8c4630927679c", "11.2.0.4 (11g Release 2)"); // JDK 5
        oracleDriverHashes.put("a2348e4944956fac05235f7cd5d30bf872afb157", "12.1.0.1 (12c Release 1)"); // JDK 7-8
        oracleDriverHashes.put("5d5d3e7a6b217ddc0c1c4c6d14b352e8b04453ef", "12.1.0.1 (12c Release 1)"); // JDK 6
        oracleDriverHashes.put("7c9b5984b2c1e32e7c8cf3331df77f31e89e24c2", "12.1.0.2 (12c Release 1)"); // JDK 7-8
        oracleDriverHashes.put("76f2f84c383ef45832b3eea6b5fb3a6edb873b93", "12.1.0.2 (12c Release 1)"); // JDK 6
        oracleDriverHashes.put("60f439fd01536508df32658d0a416c49ac6f07fb", "12.2.0.1 (12c Release 2)"); // JDK 8
        oracleDriverHashes.put("4acaa9ab2b7470fa80f0a8ec416d7ea86608ac8c", "18.3 (18c)"); // JDK 8
        oracleDriverHashes.put("bba59347e68c9416d14fcc9a9209e869f842e48d", "19.3 (19c)"); // JDK 10
        oracleDriverHashes.put("967c0b1a2d5b1435324de34a9b8018d294f8f47b", "19.3 (19c)"); // JDK 8

        String jarFile = "";
        try {
            final Class<?> generic;
            generic = Class.forName(oracleImport, false, this.getClass().getClassLoader());
            jarFile = generic.getProtectionDomain().getCodeSource().getLocation().getPath();
            LOG.info("Oracle JDBC driver loaded from '%s'", jarFile);
        } catch (ClassNotFoundException ex) {
            String msg = String.format("Oracle JDBC driver not found, unable to load class '%s'%n"
                        + "Oracle Plugin JAR files can be downloaded from https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html%n"
                        + "JAR files should be placed in the presto plugin directory", oracleImport);
            RuntimeException ex2 = new RuntimeException(msg);
            ex2.initCause(ex);
            LOG.error(ex2);
        }

        try {
            String jarSha1 = createChecksum(jarFile);
            String jarVersion = oracleDriverHashes.getOrDefault(jarSha1, "unknown");
            LOG.info("Oracle Driver: %s, sha1: %s", jarVersion, jarSha1);
        } catch (Exception ex) {
            LOG.info("Unable to check Oracle JAR Version: %s", ex.toString());
        }
    }

    private static String createChecksum(String filename) throws Exception {
        InputStream fis =  new FileInputStream(filename);
        try {
            byte[] buffer = new byte[1024 * 10];
            MessageDigest complete = MessageDigest.getInstance("SHA-1");
            int numRead;

            do {
                numRead = fis.read(buffer);
                if (numRead > 0) {
                    complete.update(buffer, 0, numRead);
                }
            } while (numRead != -1);

            // convert the sha1 hash to HEX
            byte[] hashBytes = complete.digest();
            String hashHex = HashCode.fromBytes(hashBytes).toString();
            return hashHex;
        } finally {
            // close the input stream quietly
            try {if (fis != null) fis.close();} catch(IOException ex) {}
        }
    }
}
