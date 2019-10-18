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

import java.io.File;
import java.net.URL;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
//import com.facebook.presto.server.PluginManagerConfig;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class OracleConnectorFactory
        implements ConnectorFactory
{
    private static final Logger LOG = Logger.get(OracleConnectorFactory.class);
    private final String name;
    private final String ORACLE_IMPORT_TEST = "oracle.jdbc.driver.OracleDriver";
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
        return new JdbcHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        requireOracleDriver();

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new OracleModule(connectorId), module);

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(OracleConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * The Oracle Driver is provided for us via a JAR present in the classpath.
     * If it's not present raise a useful error.
     */
    private void requireOracleDriver() {
        String overrideImport = System.getProperty(ORACLE_IMPORT_TEST);
        String oracleImport = ORACLE_IMPORT_PROP;

        // setInstalledPluginsDir
        // @Config("plugin.dir")
        // com.facebook.airlift.configuration.Config;
        // pluginManagerConfig expected = new PluginManagerConfig()
        //
        // .setInstalledPluginsDir(new File("plugins-dir"))

        // https://github.com/prestodb/presto/blob/8d5d5e67e1e2276e9e2a1fc02f471e6d0a020c89/presto-raptor/src/test/java/com/facebook/presto/raptor/RaptorQueryRunner.java
        //import com.facebook.presto.metadata.SessionPropertyManager;

        //File plugin_path = new PluginManagerConfig().getInstalledPluginsDir();
        //String pluginDir = config.getInstalledPluginsDir();

        if (overrideImport != null && !overrideImport.isEmpty()) {
            oracleImport = overrideImport;
        }
        try {
            final Class<?> generic;
            generic = Class.forName(oracleImport, false, this.getClass().getClassLoader()); // forName(String name, boolean initialize, ClassLoader loader)
            URL location = generic.getProtectionDomain().getCodeSource().getLocation();
            LOG.info("loading Oracle jdbc driver from '%s'", location);
        } catch (ClassNotFoundException ex) {
            String.format("Oracle JDBC driver not found, unable to load class %s.%n"
                        + "Oracle Plugin JAR files can be downloaded from https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
                        + "JAR files should be placed in prestos plugin directory ");
            RuntimeException ex2 = new RuntimeException(ex.getMessage());
            ex2.initCause(ex);
            throw ex2;
        }
    }

    // SHA1 hashes
    // 60f439fd01536508df32658d0a416c49ac6f07fb
    //
}
