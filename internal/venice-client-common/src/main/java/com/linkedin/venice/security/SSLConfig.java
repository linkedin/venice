package com.linkedin.venice.security;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;

import java.util.Properties;


public class SSLConfig {
  private String _keyStoreData = "";
  private String _keyStorePassword = "";
  private String _keyStoreType = "jks";
  private String _keyStoreFilePath = "";
  private String _trustStoreFilePath = "";
  private String _trustStoreFilePassword = "";
  private boolean _sslEnabled = false;
  private boolean _sslRequireClientCerts = true;
  private boolean _requireClientCertOnLocalHost = false;

  public void setKeyStoreData(String keyStoreData) {
    _keyStoreData = keyStoreData;
  }

  public String getKeyStoreData() {
    return _keyStoreData;
  }

  public void setKeyStoreFilePath(String keyStoreFilePath) {
    _keyStoreFilePath = keyStoreFilePath;
  }

  public String getKeyStoreFilePath() {
    return _keyStoreFilePath;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    _keyStorePassword = keyStorePassword;
  }

  public String getKeyStorePassword() {
    return SSLConfig.ConfigHelper.getRequired(_keyStorePassword);
  }

  public void setTrustStoreFilePath(String trustStoreFilePath) {
    _trustStoreFilePath = trustStoreFilePath;
  }

  public String getTrustStoreFilePath() {
    return _trustStoreFilePath;
  }

  public void setTrustStoreFilePassword(String trustStoreFilePassword) {
    _trustStoreFilePassword = trustStoreFilePassword;
  }

  public String getTrustStoreFilePassword() {
    return _trustStoreFilePassword;
  }

  public void setKeyStoreType(String keyStoreType) {
    _keyStoreType = keyStoreType;
  }

  public String getKeyStoreType() {
    return _keyStoreType;
  }

  public void setSslEnabled(boolean sslEnabled) {
    _sslEnabled = sslEnabled;
  }

  public boolean getSslEnabled() {
    return SSLConfig.ConfigHelper.getRequired(_sslEnabled);
  }

  public boolean doesSslRequireClientCerts() {
    return _sslRequireClientCerts;
  }

  public void setSslRequireClientCerts(boolean sslRequireClientCerts) {
    _sslRequireClientCerts = sslRequireClientCerts;
  }

  public boolean isRequireClientCertOnLocalHost() {
    return _requireClientCertOnLocalHost;
  }

  public void setRequireClientCertOnLocalHost(boolean requireClientCertOnLocalHost) {
    _requireClientCertOnLocalHost = requireClientCertOnLocalHost;
  }

  /**
   * Build a Config class from Properties that contains all SSL settings
   */
  public static SSLConfig buildConfig(Properties sslProperties) {
    SSLConfig config = new SSLConfig();
    config.setSslEnabled(Boolean.valueOf(sslProperties.getProperty(SSL_ENABLED)));
    config.setKeyStoreType(sslProperties.getProperty(SSL_KEYSTORE_TYPE));
    config.setKeyStoreFilePath(sslProperties.getProperty(SSL_KEYSTORE_LOCATION));
    config.setTrustStoreFilePath(sslProperties.getProperty(SSL_TRUSTSTORE_LOCATION));
    config.setKeyStorePassword(sslProperties.getProperty(SSL_KEYSTORE_PASSWORD));
    config.setTrustStoreFilePassword(sslProperties.getProperty(SSL_TRUSTSTORE_PASSWORD));
    return config;
  }

  public static class ConfigHelper {
    private ConfigHelper() {
    }

    public static Object getRequiredObject(Object o) throws MissingConfigParameterException {
      if (o == null) {
        throw new MissingConfigParameterException("required Object has not been defined");
      } else {
        return o;
      }
    }

    public static <T> T getRequired(T o) throws MissingConfigParameterException {
      if (o == null) {
        throw new MissingConfigParameterException("required Object has not been defined");
      } else {
        return o;
      }
    }

    public static class MissingConfigParameterException extends IllegalArgumentException {
      public MissingConfigParameterException(String msg) {
        super(msg);
      }
    }
  }
}
