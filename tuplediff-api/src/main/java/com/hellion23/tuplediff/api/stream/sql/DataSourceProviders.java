package com.hellion23.tuplediff.api.stream.sql;

import com.hellion23.tuplediff.api.model.TDException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

/**
 *
 * Created by hleung on 5/29/2017.
 */
@Slf4j
public class DataSourceProviders {
    private static final String TNS_PROPERTY = "oracle.net.tns_admin";
    private static final String p6SpyDriver = "com.p6spy.engine.spy.P6SpyDriver";
    private static StandardPBEStringEncryptor encryptor;
    private static String propertiesDir;
    private static String tnsLocation;

    private static Map<String, Supplier<DataSource> > datasources = new HashMap<>();
    private static Map<String, Properties> allDBProperties = null;

    static {
        // For automatic password decryption
        encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("a27361ec1f0cb12c9faf8289aa0abcb3");
        encryptor.setAlgorithm("PBEWITHSHA1ANDDESEDE");

        // windows TNSLocation
        tnsLocation = "C:\\Oracle\\etc";
        // Windows properties Directory.
        propertiesDir = "N:/NYC/proj/dev/apps/db/properties";

        if (SystemUtils.IS_OS_UNIX ) {
            // Unix TNSLocation
            tnsLocation = "/var/opt/oracle/etc";
            // Unix properties location:
            propertiesDir = "/proj/dev/apps/db/properties";
            // Oracle driver calls a blocking unix RNG for seed generation:
            // https://support.oracle.com/knowledge/Middleware/1594701_1.html
            if (System.getProperty("java.security.egd") == null) {
                System.setProperty("java.security.egd", "file:///dev/urandom" );
            }
        }

        // For thin ORACLE clients, setup TNS location.
        if (System.getProperty(TNS_PROPERTY) == null) {
            System.setProperty(TNS_PROPERTY, tnsLocation);
        }

    }


    public static Set<String> allDbKeys() {
        return getAllDBProperties().keySet();
    }

    private static Map<String, Properties> getAllDBProperties () {
        if (allDBProperties == null) {
           initAllDBProperties(true);
        }
        return allDBProperties;
    }

    private static Properties getDBProperties (String propertyFileName) {
        if (allDBProperties == null || !allDBProperties.containsKey(propertyFileName)) {
			initAllDBProperties(true);
        }
        return allDBProperties.get(propertyFileName);
    }

    /**
     * (Re) initialize all DB properties. First pulls properties in /proj/dev/apps/db/properties, then if failing that
     * tries the classpath.
     *
     * @param forceReload If set to true, will discard cache of properties and reload all. If set to false, will only
     *                    pick up new Properties files in directories.
     *
     */
    private static void initAllDBProperties(boolean forceReload) {
        if (forceReload) {
            allDBProperties = new HashMap<>();
        }
        loadDBPropertiesFromLocation("*.properties");
    }

    /**
     * Load properties from known resource locations.
     *
     * @param pattern File format filter, e.g. *.properties
     */
    private static void loadDBPropertiesFromLocation(String pattern) {
        if (allDBProperties == null) {
            allDBProperties = new HashMap<>();
        }
        try {
            String resourcePath = "file:/" + propertiesDir + "/" + pattern;

			Resource[] resources = new PathMatchingResourcePatternResolver().getResources(resourcePath);
			for (Resource resource : resources) {
				Properties properties = PropertiesLoaderUtils.loadProperties(resource);
				// Validate the properties file is actually a DB property file by checking that the file
				// has the requisite property keys.
				if (properties.getProperty("url")!=null ) {
					allDBProperties.put(resource.getFilename(), properties);
				}
			}
			log.info(String.format("Got %s DB .properties files from resource path: %s", resources.length, resourcePath));

        } catch (Exception e) {
            log.error("Could not enumerate all properties files from classpath!", e);
        }
    }

    /**
     * Create DataSource Supplier with given property file name. PropertyFile needs to exist in
     * /proj/dev/apps/db/properties (or N:/NYC/proj/dev/apps/db/properties if windows) or in the classpath, looking
     * up in that order.
     *
     * @param propertyFileName db property file name, e.g. dwv2-production.properties
     * @return Supplier for dataSource
     */
    public static Supplier<DataSource> getDb(String propertyFileName) {
        if (!propertyFileName.endsWith(".properties")) {
            propertyFileName = propertyFileName + ".properties";
        }
        Properties props = getDBProperties(propertyFileName);
        if (props == null) {
            throw new IllegalArgumentException("DB Property file name " + propertyFileName + " not found in classpath" +
                    " or default property location ");
        }
        return datasources.computeIfAbsent(propertyFileName, k -> new HikariDataSourceProvider(props) );
    }

    /**
     * Get a DataSource Supplier with provided url, user, password. If properties object isn't null, will use
     * whatever is defined in the Properties object.
     *
     * @param url db url
     * @param user username
     * @param password password
     * @param properties additional DB properties
     * @return
     */
    public static Supplier<DataSource> getDb (String url, String user, String password, Properties properties) {
        url = url == null && properties != null ? properties.getProperty("url") : url;
        user = user == null && properties != null? properties.getProperty("username") : user;
        password = password == null && properties != null? properties.getProperty("password") : password;
        String driverClassName = properties != null ? properties.getProperty("driverClassName") : null;
        String validationQuery = properties != null ? properties.getProperty("validationQuery") : null;
        String poolName = properties != null ? properties.getProperty("poolName") : url;
        String catalogName = properties != null ? properties.getProperty("db") : null;

        Supplier<DataSource> dataSource = datasources.get(poolName);
        if (dataSource == null) {
            dataSource = new HikariDataSourceProvider (poolName, url, user, password, driverClassName, validationQuery, catalogName);
            datasources.put(poolName, dataSource);
        }
        return dataSource;
    }

    public static class HikariDataSourceProvider implements Supplier<DataSource> {
        HikariDataSource dataSource = null;
        String poolName, url, user, password, validationQuery, driverClassName, catalogName;

        public HikariDataSourceProvider( Properties props ) {
            this(null, props);
        }

        public HikariDataSourceProvider (String poolName, Properties props) {
            this(poolName == null ? props.getProperty("url") : poolName,
                    props.getProperty("url"),
                    props.getProperty("username"),
                    props.getProperty("password"),
                    props.getProperty("driverClassName"),
                    props.getProperty("validationQuery"),
                    props.getProperty("db")
                    );
        }

        public HikariDataSourceProvider(String url, String user, String password) {
            this (url, url, user, password, p6SpyDriver, null, null);
        }

        public HikariDataSourceProvider(String poolName, String url, String user, String password, String driverClassName, String validationQuery, String catalogName) {
            this.poolName = poolName;
            this.url = url;
            this.password = password;
            this.user = user;
            this.validationQuery = validationQuery;
            this.driverClassName = driverClassName;
            this.catalogName = catalogName;
        }

        @Override
        public DataSource get() {
            if (dataSource == null) {
                try {
                    HikariConfig config = new HikariConfig();
                    config.setPoolName(poolName);
                    config.setDriverClassName(driverClassName);
                    config.setJdbcUrl(convertUrlToP6Spy(url, driverClassName));
                    config.setUsername(user);
                    config.setPassword(decryptPassword(password));
                    config.setCatalog (catalogName);

                    // Validation Queries are no longer necessary if using JDBC4; this is deprecated. Uncomment if there
                    // are any issues.
//                    if (validationQuery != null && !"".equals(validationQuery)) {
//                        config.setConnectionTestQuery(validationQuery);
//                    }

                    // Set pool max size and idle size. We want the pool to have no open connections when no one is making
                    // queries.
                    config.setMaximumPoolSize(10);
                    config.setMinimumIdle(0);

                    // Remove connection from pool if unused for 15 mintues:
                    config.setIdleTimeout(Duration.ofMinutes(15).toMillis());

                    // 30 minute leak detection. Unlike Tomcat, Hikari will not remove abandoned connections, but will
                    // instead just do an Exception throw.
                    config.setLeakDetectionThreshold(Duration.ofMinutes(30).toMillis());
                    // 5 second timeout
                    dataSource = new HikariDataSource(config);

                } catch (Exception e) {
                    throw new TDException("Could not instantiate DB url " + url + " Reason " + e.getMessage());
                }
            }
            return  dataSource;
        }

        private String decryptPassword (String password) {
            String decrypted = password;
            if (password != null && password.startsWith ("ENC(") && password.endsWith(")")) {
                String value = password.substring("ENC(".length(), password.length() - ")".length());
                decrypted = encryptor.decrypt(value);
            }
            return decrypted;
        }

        private String convertUrlToP6Spy (String url, String driver) {
            if (p6SpyDriver.equals(driver) && !url.contains("p6spy")) {
                url = url.replaceFirst("jdbc:", "jdbc:p6spy:");
            }
            return url;
        }
    }

}
