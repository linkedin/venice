package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains some utils methods for VPJ jobs to obtain SSL-related configs.
 */
public class VPJSSLUtils {
  private static final Logger LOGGER = LogManager.getLogger(VPJSSLUtils.class);

  /**
   * Build a ssl properties based on the hadoop token file.
   */
  public static Properties getSslProperties(VeniceProperties allProperties) throws IOException {
    Properties newSslProperties = new Properties();
    // SSL_ENABLED is needed in SSLFactory
    newSslProperties.setProperty(SSL_ENABLED, "true");
    newSslProperties.setProperty(PUBSUB_SECURITY_PROTOCOL, PubSubSecurityProtocol.SSL.name());
    allProperties.keySet()
        .stream()
        .filter(key -> key.toLowerCase().startsWith(SSL_PREFIX))
        .forEach(key -> newSslProperties.setProperty(key, allProperties.getString(key)));
    SSLConfigurator sslConfigurator = SSLConfigurator.getSSLConfigurator(
        allProperties.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));

    Properties sslWriterProperties =
        sslConfigurator.setupSSLConfig(newSslProperties, UserCredentialsFactory.getUserCredentialsFromTokenFile());
    newSslProperties.putAll(sslWriterProperties);
    newSslProperties.put(SSL_CONFIGURATOR_CLASS_CONFIG, sslConfigurator.getClass().getName());
    return newSslProperties;
  }

  /**
   * JVM-level cache of the materialized SSL properties (keystore/truststore temp file locations, etc).
   * The Hadoop token file and SSL configurator class are fixed for the lifetime of an executor JVM, so
   * the (potentially expensive) token-file read and temp-file materialization only need to happen once
   * per executor, no matter how many times {@link #setupSSLForExecutor(VeniceProperties)} is called
   * (e.g. once per Spark task, or once per key group). Failures are never cached so a transient issue
   * (e.g. token file not yet mounted) can be retried on a subsequent call.
   */
  private static volatile Properties cachedExecutorSslProps = null;
  private static final Object EXECUTOR_SSL_PROPS_LOCK = new Object();

  /**
   * Sets up SSL on the executor side before creating a PubSub consumer, by materializing SSL properties
   * from the Hadoop token file. This is a no-op when no SSL configurator class is configured. The
   * materialized SSL properties are cached per-JVM (see {@link #cachedExecutorSslProps}) so repeated
   * calls on the same executor don't re-read the token file or re-write temp certificate files.
   */
  public static VeniceProperties setupSSLForExecutor(VeniceProperties config) {
    if (!config.containsKey(SSL_CONFIGURATOR_CLASS_CONFIG)) {
      return config;
    }
    try {
      Properties sslProps = getCachedExecutorSslProperties(config);
      Properties merged = config.toProperties();
      merged.putAll(sslProps);
      return new VeniceProperties(merged);
    } catch (Exception e) {
      String msg = "Failed to setup SSL for executor-side PubSub client creation. "
          + "Ensure the Hadoop token file is accessible and SSL certificates are valid. SSL configurator class: "
          + config.getString(SSL_CONFIGURATOR_CLASS_CONFIG);
      LOGGER.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  private static Properties getCachedExecutorSslProperties(VeniceProperties config) throws IOException {
    Properties cached = cachedExecutorSslProps;
    if (cached != null) {
      return cached;
    }
    synchronized (EXECUTOR_SSL_PROPS_LOCK) {
      cached = cachedExecutorSslProps;
      if (cached == null) {
        cached = getSslProperties(config);
        cachedExecutorSslProps = cached;
      }
      return cached;
    }
  }

  /**
   * Test-only hook to reset the per-JVM SSL properties cache between test cases.
   */
  @VisibleForTesting
  public static void resetExecutorSslPropsCacheForTests() {
    synchronized (EXECUTOR_SSL_PROPS_LOCK) {
      cachedExecutorSslProps = null;
    }
  }

  public static void validateSslProperties(VeniceProperties props) {
    String[] requiredSSLPropertiesNames = new String[] { SSL_KEY_PASSWORD_PROPERTY_NAME,
        SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, SSL_KEY_STORE_PROPERTY_NAME, SSL_TRUST_STORE_PROPERTY_NAME };
    for (String sslPropertyName: requiredSSLPropertiesNames) {
      if (!props.containsKey(sslPropertyName)) {
        throw new VeniceException("Miss the require ssl property name: " + sslPropertyName);
      }
    }
  }

  public static Optional<SSLFactory> createSSLFactory(
      final boolean enableSsl,
      final String sslFactoryClassName,
      final Lazy<Properties> sslProps) {
    Optional<SSLFactory> sslFactory = Optional.empty();
    if (enableSsl) {
      LOGGER.info("Controller ACL is enabled.");
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProps.get(), sslFactoryClassName));
    }
    return sslFactory;
  }
}
