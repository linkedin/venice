package com.linkedin.venice.security;

import static com.linkedin.venice.CommonConfigKeys.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;


/**
 * Cloned from {@link com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl};
 *
 * Changes:
 * 1. Added a new constructor that accepts {@link Properties}; in the venice backend product, we should wrap around
 *    the LinkedIn internal SSL factory, add a new constructor that accepts {@link Properties} and plug in the wrapper
 *    into Venice;
 * 2. Added a helper function that builds a {@link Config} from {@link Properties}
 */
public class DefaultSSLFactory implements SSLFactory {
  public static final String[] CIPHER_SUITE_ALLOWLIST = {
      // Preferred ciphersuites:
      "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_GCM_SHA256",
      // For java 1.6 support:
      "TLS_RSA_WITH_AES_128_CBC_SHA",
      // the remaining are for backwards compatibility and shouldn't be used by newer clients
      "SSL_RSA_WITH_NULL_MD5", "SSL_RSA_WITH_NULL_SHA" };
  private SSLContext _context;
  private boolean _sslEnabled;
  private boolean _sslRequireClientCerts;
  private String _keyStoreFilePath;
  private String _trustStoreFilePath;
  @Deprecated
  private String _keyStoreData;
  private SSLParameters _parameters;

  public DefaultSSLFactory(Properties sslProperties) throws Exception {
    this(buildConfig(sslProperties));
  }

  public DefaultSSLFactory(Config config) throws Exception {
    _sslEnabled = config.getSslEnabled();
    if (_sslEnabled) {
      _keyStoreFilePath = config.getKeyStoreFilePath();
      _trustStoreFilePath = config.getTrustStoreFilePath();
      _keyStoreData = config.getKeyStoreData();

      // Either keyStoreFilePath + trustStoreFilePath or keyStoreData must be provided.
      if (StringUtils.isNotBlank(_keyStoreData)) {
        _context = new SSLContextFactory(_keyStoreData, config.getKeyStorePassword()).getContext();
      } else if (StringUtils.isNotBlank(_keyStoreFilePath) && StringUtils.isNotBlank(_trustStoreFilePath)) {
        _context = new SSLContextFactory(
            new File(_keyStoreFilePath),
            config.getKeyStorePassword(),
            config.getKeyStoreType(),
            new File(_trustStoreFilePath),
            config.getTrustStoreFilePassword()).getContext();
      } else {
        throw new ConfigHelper.MissingConfigParameterException(
            "Either keyStoreData or (keyStoreFilePath and trustStoreFilePath) must be provided to operate in sslEnabled mode.");
      }
      String[] allowedCiphersuites =
          filterDisallowedCiphersuites(_context.getSocketFactory().getSupportedCipherSuites());

      _parameters = _context.getDefaultSSLParameters();
      _parameters.setCipherSuites(allowedCiphersuites);

      if (config.doesSslRequireClientCerts()) {
        _parameters.setNeedClientAuth(true);
      } else {
        _parameters.setWantClientAuth(true);
      }
    } else {
      _context = null;
      _parameters = null;
    }
  }

  public static String[] filterDisallowedCiphersuites(String[] ciphersuites) throws SSLProtocolException {
    Set<String> allowedCiphers = new HashSet<String>();
    Collections.addAll(allowedCiphers, CIPHER_SUITE_ALLOWLIST);

    Set<String> supportedCiphers = new HashSet<String>();
    Collections.addAll(supportedCiphers, ciphersuites);

    supportedCiphers.retainAll(allowedCiphers);
    String[] allowedCiphersuites = supportedCiphers.toArray(new String[0]);

    if (allowedCiphersuites == null || allowedCiphersuites.length == 0) {
      throw new SSLProtocolException("No Allowlisted SSL Ciphers Available.");
    }

    return allowedCiphersuites;
  }

  @Override
  public SSLContext getSSLContext() {
    return _context;
  }

  @Override
  public SSLParameters getSSLParameters() {
    return _parameters;
  }

  @Override
  public boolean isSslEnabled() {
    return _sslEnabled;
  }

  public boolean isSslRequireClientCerts() {
    return _sslRequireClientCerts;
  }

  public void setSslRequireClientCerts(boolean sslRequireClientCerts) {
    _sslRequireClientCerts = sslRequireClientCerts;
  }

  public static class Config {
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
      return ConfigHelper.getRequired(_keyStorePassword);
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
      return ConfigHelper.getRequired(_sslEnabled);
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
  }

  /**
   * Build a Config class from Properties that contains all SSL settings
   */
  private static Config buildConfig(Properties sslProperties) {
    Config config = new Config();
    config.setSslEnabled(Boolean.valueOf(sslProperties.getProperty(SSL_ENABLED)));
    config.setKeyStoreType(sslProperties.getProperty(SSL_KEYSTORE_TYPE));
    config.setKeyStoreFilePath(sslProperties.getProperty(SSL_KEYSTORE_LOCATION));
    config.setTrustStoreFilePath(sslProperties.getProperty(SSL_TRUSTSTORE_LOCATION));
    config.setKeyStorePassword(sslProperties.getProperty(SSL_KEYSTORE_PASSWORD));
    config.setTrustStoreFilePassword(sslProperties.getProperty(SSL_TRUSTSTORE_PASSWORD));
    return config;
  }

  /**
   * Cloned from {@link com.linkedin.security.ssl.access.control.SSLContextFactory}; this is made as a private class
   * of DefaultSSLFactory on purpose.
   */
  private class SSLContextFactory {
    private SSLContext _secureContext = null;
    private static final String DEFAULT_ALGORITHM = "SunX509";
    private static final String DEFAULT_PROTOCOL = "TLS";
    private static final String JKS_STORE_TYPE_NAME = "JKS";
    private static final String P12_STORE_TYPE_NAME = "PKCS12";

    @Deprecated
    /**
     * This constructor uses keyStoreData for both keyStore and trustStore.
     * It only supports base64 encoded jks format keyStore.
     */
    SSLContextFactory(String keyStoreData, String keyStorePassword) throws Exception {
      // load they keystore
      KeyStore certKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      certKeyStore.load(toInputStream(keyStoreData), keyStorePassword.toCharArray());

      // set Keymanger toInputStream() use X509
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
      kmf.init(certKeyStore, keyStorePassword.toCharArray());

      // use a standard trust manager
      TrustManagerFactory trustFact = TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
      trustFact.init(certKeyStore);

      // set context to TLS and initialize it
      _secureContext = SSLContext.getInstance(DEFAULT_PROTOCOL);
      _secureContext.init(kmf.getKeyManagers(), trustFact.getTrustManagers(), null);
    }

    /**
     * The keyStoreFile takes a File object of p12 or jks file depends on keyStoreType
     * The trustStoreFile always takes a File object of JKS file.
     */
    SSLContextFactory(
        File keyStoreFile,
        String keyStorePassword,
        String keyStoreType,
        File trustStoreFile,
        String trustStorePassword) throws Exception {
      if (!keyStoreType.equalsIgnoreCase(P12_STORE_TYPE_NAME) && !keyStoreType.equalsIgnoreCase(JKS_STORE_TYPE_NAME)) {
        throw new Exception("Unsupported keyStoreType: " + keyStoreType);
      }

      // Load KeyStore
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(toInputStream(keyStoreFile), keyStorePassword.toCharArray());

      // Load TrustStore
      KeyStore trustStore = KeyStore.getInstance(JKS_STORE_TYPE_NAME);
      trustStore.load(toInputStream(trustStoreFile), trustStorePassword.toCharArray());

      // Set KeyManger from keyStore
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
      kmf.init(keyStore, keyStorePassword.toCharArray());

      // Set TrustManager from trustStore
      TrustManagerFactory trustFact = TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
      trustFact.init(trustStore);

      // Set Context to TLS and initialize it
      _secureContext = SSLContext.getInstance(DEFAULT_PROTOCOL);
      _secureContext.init(kmf.getKeyManagers(), trustFact.getTrustManagers(), null);
    }

    private InputStream toInputStream(String storeData) {
      byte[] data = Base64.getDecoder().decode(storeData);
      return new ByteArrayInputStream(data);
    }

    private InputStream toInputStream(File storeFile) throws IOException {
      byte[] data = FileUtils.readFileToByteArray(storeFile);
      return new ByteArrayInputStream(data);
    }

    SSLContext getContext() {
      return _secureContext;
    }
  }

  private static class ConfigHelper {
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
