package com.linkedin.venice;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_NEEDS_CLIENT_CERT;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;


public class SSLConfig {
  public static final boolean DEFAULT_CONTROLLER_SSL_ENABLED = true;

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
  private boolean sslNeedsClientCert;
  private boolean controllerSSLEnabled;

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
    sslNeedsClientCert = veniceProperties.getBoolean(SSL_NEEDS_CLIENT_CERT, false);
    controllerSSLEnabled = veniceProperties.getBoolean(CONTROLLER_SSL_ENABLED, DEFAULT_CONTROLLER_SSL_ENABLED);
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

  public com.linkedin.venice.security.SSLConfig getSSLConfig() {
    com.linkedin.venice.security.SSLConfig config = new com.linkedin.venice.security.SSLConfig();
    config.setSslEnabled(true);
    config.setKeyStoreFilePath(sslKeyStoreLocation);
    config.setKeyStorePassword(sslKeyStorePassword);
    config.setKeyStoreType(sslKeyStoreType);
    config.setTrustStoreFilePassword(sslTrustStorePassword);
    config.setTrustStoreFilePath(sslTrustStoreLocation);
    config.setTrustStoreType(sslTrustStoreType);

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

  public boolean isSslNeedsClientCert() {
    return sslNeedsClientCert;
  }

  public boolean isControllerSSLEnabled() {
    return controllerSSLEnabled;
  }

  public String getSslKeyStoreType() {
    return sslKeyStoreType;
  }

  public String getSslKeyPassword() {
    return sslKeyPassword;
  }

  public String getSslKeyManagerAlgorithm() {
    return sslKeyManagerAlgorithm;
  }

  public String getSslTrustManagerAlgorithm() {
    return sslTrustManagerAlgorithm;
  }

  public String getSslSecureRandomImplementation() {
    return sslSecureRandomImplementation;
  }
}
