package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
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
    newSslProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
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
