package com.linkedin.venice;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.common.config.SslConfigs;

import static com.linkedin.venice.CommonConfigKeys.*;


public class SSLConfig {
  private String sslKeyStoreLocation;
  private String sslKeyStorePassword;
  private String sslKeyStoreType;
  private String sslKeyPassword;
  private String sslTrustStoreLocation;
  private String sslTrustStorePassword;
  private String sslTrustStoreType;
  private String sslKeyManagerAlgorithm;
  private String sslTrustManagerAlgorithm;
  private String sslSecureRandomImplementation;

  public SSLConfig(VeniceProperties veniceProperties) {
    // The following configs are required for SSL support
    sslKeyStoreLocation = veniceProperties.getString(SSL_KEYSTORE_LOCATION);
    sslKeyStorePassword = veniceProperties.getString(SSL_KEYSTORE_PASSWORD);
    sslKeyStoreType = veniceProperties.getString(SSL_KEYSTORE_TYPE);
    sslKeyPassword = veniceProperties.getString(SSL_KEY_PASSWORD);
    sslTrustStoreLocation = veniceProperties.getString(SSL_TRUSTSTORE_LOCATION);
    sslTrustStorePassword = veniceProperties.getString(SSL_TRUSTSTORE_PASSWORD);
    sslTrustStoreType = veniceProperties.getString(SSL_TRUSTSTORE_TYPE);
    sslKeyManagerAlgorithm = veniceProperties.getString(SSL_KEYMANAGER_ALGORITHM);
    sslTrustManagerAlgorithm = veniceProperties.getString(SSL_TRUSTMANAGER_ALGORITHM);
    sslSecureRandomImplementation = veniceProperties.getString(SSL_SECURE_RANDOM_IMPLEMENTATION);
  }

  /**
   * @return An instance of {@link Properties} for Kafka clients.
   */
  public Properties getKafkaSSLConfig() {
    Properties kafkaSSLConfig = new Properties();
    kafkaSSLConfig.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeyStoreLocation);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeyStorePassword);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslKeyStoreType);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTrustStoreLocation);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTrustStorePassword);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTrustStoreType);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, sslKeyManagerAlgorithm);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, sslTrustManagerAlgorithm);
    kafkaSSLConfig.setProperty(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, sslSecureRandomImplementation);

    return kafkaSSLConfig;
  }

  /**
   *
   * @return An instance of {@link Properties} from SSL config.
   */
  public Properties getSslProperties() {
    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED, "true");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, sslKeyStoreType);
    sslProperties.setProperty(SSL_KEYSTORE_LOCATION, sslKeyStoreLocation);
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, sslKeyStorePassword);
    sslProperties.setProperty(SSL_TRUSTSTORE_TYPE, sslTrustStoreType);
    sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, sslTrustStoreLocation);
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, sslTrustStorePassword);
    return sslProperties;
  }

  public SSLEngineComponentFactoryImpl.Config getSslEngineComponentConfig(){
    SSLEngineComponentFactoryImpl.Config config = new SSLEngineComponentFactoryImpl.Config();
    config.setSslEnabled(true);
    config.setKeyStoreFilePath(sslKeyStoreLocation);
    config.setKeyStorePassword(sslKeyStorePassword);
    config.setKeyStoreType(sslKeyStoreType);
    config.setTrustStoreFilePassword(sslTrustStorePassword);
    config.setTrustStoreFilePath(sslTrustStoreLocation);

    return config;
  }

  public String getSslKeyStoreLocation() {
    return sslKeyStoreLocation;
  }

  public String getSslKeyStorePassword() {
    return sslKeyStorePassword;
  }

  public String getSslTrustStoreLocation() {
    return sslTrustStoreLocation;
  }

  public String getSslTrustStorePassword() {
    return sslTrustStorePassword;
  }
}
