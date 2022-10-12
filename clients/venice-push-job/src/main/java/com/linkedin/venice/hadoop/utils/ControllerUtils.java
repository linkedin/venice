package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_PREFIX;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_URL_PROP;

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
 * This class contains some utils methods for H2V jobs to obtain controller/SSL-related configs.
 */
public class ControllerUtils {
  private static final Logger LOGGER = LogManager.getLogger(ControllerUtils.class);

  public static String getVeniceControllerUrl(VeniceProperties props) {
    String veniceControllerUrl = null;
    if (!props.containsKey(VENICE_URL_PROP) && !props.containsKey(VENICE_DISCOVER_URL_PROP)) {
      throw new VeniceException(
          "At least one of the following config properties needs to be present: " + VENICE_URL_PROP + " or "
              + VENICE_DISCOVER_URL_PROP);
    }
    if (props.containsKey(VENICE_URL_PROP)) {
      veniceControllerUrl = props.getString(VENICE_URL_PROP);
    }
    if (props.containsKey(VENICE_DISCOVER_URL_PROP)) {
      /**
       * {@link VENICE_DISCOVER_URL_PROP} has higher priority than {@link VENICE_URL_PROP}.
       */
      veniceControllerUrl = props.getString(VENICE_DISCOVER_URL_PROP);
    }
    return veniceControllerUrl;
  }

  /**
   * Build a Lazy ssl properties based on the hadoop token file.
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
    return newSslProperties;
  }

  public static Optional<SSLFactory> createSSlFactory(
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
