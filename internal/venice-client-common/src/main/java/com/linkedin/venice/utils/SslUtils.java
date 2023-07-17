package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;

import com.linkedin.alpini.base.ssl.SslFactory;
import com.linkedin.alpini.netty4.ssl.SSLEngineFactoryImpl;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.conscrypt.Conscrypt;


public class SslUtils {
  private static final Logger LOGGER = LogManager.getLogger(SslUtils.class);

  /**
   * Self-signed cert. Use keystore as truststore since self-signed. Cert has CN=localhost
   *
   * IMPORTANT NOTE: the "localhost.jks", "localhost.cert", "localhost.key" and "localhost.p12" files only exist
   *                 in the code base; do not try to load this files in actual hosts
   */
  public static final String LOCAL_PASSWORD = "dev_pass";
  public static final String LOCAL_KEYSTORE_JKS = "localhost.jks";

  /**
   * This function should be used in test cases only.
   *
   * @return an instance of {@link SSLConfig} with local SSL config.
   */
  public static SSLConfig getLocalSslConfig() {
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setKeyStoreFilePath(keyStorePath);
    sslConfig.setKeyStorePassword(LOCAL_PASSWORD);
    sslConfig.setKeyStoreType("JKS");
    sslConfig.setTrustStoreFilePath(keyStorePath);
    sslConfig.setTrustStoreFilePassword(LOCAL_PASSWORD);
    sslConfig.setSslEnabled(true);
    return sslConfig;
  }

  /**
   * This function should be used in test cases only.
   *
   * @return a local SSL factory that uses a self-signed development certificate.
   */
  public static SSLFactory getVeniceLocalSslFactory() {
    Properties sslProperties = getVeniceLocalSslProperties();
    try {
      return new DefaultSSLFactory(sslProperties);
    } catch (Exception e) {
      throw new VeniceException("Failed to build Venice local SSL factory.", e);
    }
  }

  /**
   * This function should be used in test cases only.
   *
   * @return an instance of {@link Properties} that contains local SSL configs.
   */
  public static Properties getVeniceLocalSslProperties() {
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED, "true");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_KEYSTORE_LOCATION, keyStorePath);
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD);
    sslProperties.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, keyStorePath);
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD);
    sslProperties.setProperty(SSL_KEY_PASSWORD, LOCAL_PASSWORD);
    sslProperties.setProperty(SSL_KEYMANAGER_ALGORITHM, "SunX509");
    sslProperties.setProperty(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
    sslProperties.setProperty(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    return sslProperties;
  }

  public static VeniceTlsConfiguration getTlsConfiguration() {
    String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
    return SslUtils.VeniceTlsConfiguration.builder()
        .setKeyStorePath(keyStorePath)
        .setKeyStorePassword(LOCAL_PASSWORD)
        .setKeyStoreType("JKS")
        .setTrustStorePath(keyStorePath)
        .setTrustStorePassword(LOCAL_PASSWORD)
        .setTrustStoreType("JKS")
        .setKeyPassphrase(LOCAL_PASSWORD)
        .setKeyManagerAlgorithm("SunX509")
        .setTrustStoreManagerAlgorithm("SunX509")
        .setSecureRandomAlgorithm("SHA1PRNG")
        .build();
  }

  /**
   * This function should be used in test cases only.
   *
   * @param resource -- System resource name
   * @return the path to the local key store location
   */
  public static String getPathForResource(String resource) {
    String systemTempDir = System.getProperty("java.io.tmpdir");
    String subDir = "venice-keys-" + UUID.randomUUID();
    File tempDir = new File(systemTempDir, subDir);
    tempDir.mkdir();
    tempDir.deleteOnExit();
    File file = new File(tempDir.getAbsolutePath(), resource);
    if (!file.exists()) {
      try (InputStream is = (ClassLoader.getSystemResourceAsStream(resource))) {
        if (is == null) {
          throw new IllegalStateException(
              "ClassLoader.getSystemResourceAsStream returned null resource for: " + resource);
        }
        Files.copy(is, file.getAbsoluteFile().toPath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy resource: " + resource + " to tmp dir", e);
      } finally {
        file.deleteOnExit();
      }
    }
    return file.getAbsolutePath();
  }

  public static SslFactory toAlpiniSSLFactory(SSLFactory sslFactory) {
    return toAlpiniSSLFactory(sslFactory, false);
  }

  public static SslFactory toAlpiniSSLFactory(SSLFactory sslFactory, boolean openssl) {
    try {
      SSLEngineFactoryImpl.Config config = toAlpiniSSLConfig(sslFactory.getSSLConfig());
      if (openssl) {
        if (isConscryptAvailable()) {
          LOGGER.info("Constructing an openssl based SSL factory");
          config.setSslContextProvider(Conscrypt.newProvider());
        } else {
          LOGGER.info("Conscrypt is not available, fall back to the default SSL factory");
        }
      }
      return new SSLEngineFactoryImpl(config);
    } catch (Exception e) {
      throw new VeniceException("Unable to create SSL factory", e);
    }
  }

  /**
   * Adapt the incoming {@link SSLFactory} into a new one backed by openssl if it is available.
   */
  public static SSLFactory toSSLFactoryWithOpenSSLSupport(SSLFactory sslFactory) {
    if (!isConscryptAvailable()) {
      LOGGER.info("Conscrypt is not available, return the original ssl factory");
      return sslFactory;
    }
    SslFactory internalSslFactory = toAlpiniSSLFactory(sslFactory, true);
    return new SSLFactory() {
      @Override
      public SSLConfig getSSLConfig() {
        return sslFactory.getSSLConfig();
      }

      @Override
      public SSLContext getSSLContext() {
        return internalSslFactory.getSSLContext();
      }

      @Override
      public SSLParameters getSSLParameters() {
        return internalSslFactory.getSSLParameters();
      }

      @Override
      public boolean isSslEnabled() {
        return sslFactory.isSslEnabled();
      }
    };
  }

  /**
   * Check whether openssl provider is available or not.
   */
  public static boolean isConscryptAvailable() {
    try {
      Conscrypt.checkAvailability();
      return true;
    } catch (UnsatisfiedLinkError e) {
      return false;
    }
  }

  public static SSLEngineFactoryImpl.Config toAlpiniSSLConfig(SSLConfig sslConfig) {
    SSLEngineFactoryImpl.Config config = new SSLEngineFactoryImpl.Config();
    config.setSslEnabled(sslConfig.getSslEnabled());
    config.setKeyStoreType(sslConfig.getKeyStoreType());
    config.setKeyStoreData(sslConfig.getKeyStoreData());
    config.setKeyStorePassword(sslConfig.getKeyStorePassword());
    config.setKeyStoreFilePath(sslConfig.getKeyStoreFilePath());
    config.setTrustStoreFilePath(sslConfig.getTrustStoreFilePath());
    config.setTrustStoreFilePassword(sslConfig.getTrustStoreFilePassword());
    return config;
  }

  /**
   * A helper function that return an instance of {@link SSLFactory} with ssl Properties.
   * @param sslProperties
   * @param factoryClassName Different products can plug-in different factory classes.
   */
  public static SSLFactory getSSLFactory(Properties sslProperties, String factoryClassName) {
    Class<SSLFactory> factoryClass = ReflectUtils.loadClass(factoryClassName);
    Class<Properties> propertiesClass = ReflectUtils.loadClass(Properties.class.getName());
    SSLFactory sslFactory =
        ReflectUtils.callConstructor(factoryClass, new Class[] { propertiesClass }, new Object[] { sslProperties });
    return sslFactory;
  }

  /**
   * Build an instance of {@link Properties} with a file path to SSL config file.
   *
   * An example of SSL config file:
   * ssl.enabled=true
   * keystore.type=PKCS12
   * keystore.password=local_password
   * keystore.path=./identity.p12
   * truststore.password=local_password
   * truststore.path=/etc/riddler/cacerts
   */
  public static Properties loadSSLConfig(String configFilePath) throws IOException {
    Properties props = new Properties();
    try (FileInputStream inputStream = new FileInputStream(configFilePath)) {
      props.load(inputStream);
    } catch (IOException e) {
      LOGGER.error("Could not load ssl config file from path: {}", configFilePath, e);
      throw e;
    }
    return props;
  }

  public static X509Certificate getX509Certificate(Certificate certificate) {
    if (!(certificate instanceof X509Certificate)) {
      String exceptionMessage = new StringBuilder().append("Only certificates of type ")
          .append(X509Certificate.class.getName())
          .append(" are supported. Received certificate of type ")
          .append(certificate.getClass().getName())
          .toString();
      throw new IllegalArgumentException(exceptionMessage);
    }

    return (X509Certificate) certificate;
  }

  /**
   * This class is used to configure TLS for Venice components in integration tests.
   */
  public static class VeniceTlsConfiguration {
    private final String hostname;
    private final String sslProtocol;
    private final String trustStorePath;
    private final String keyStorePath;
    private final String keyStoreType;
    private final String trustStoreType;
    private final String keyStorePassword;
    private final String trustStorePassword;
    private final String keyPassphrase;
    private final String keyManagerAlgorithm;
    private final String trustManagerAlgorithm;
    private final String secureRandomAlgorithm;
    private final boolean useOpenSsl;
    private final boolean validateCertificates;
    private final boolean allowGeneratingSelfSignedCertificate;

    public String getHostname() {
      return hostname;
    }

    public String getKeyPassphrase() {
      return keyPassphrase;
    }

    public String getSslProtocol() {
      return sslProtocol;
    }

    public String getTrustStorePath() {
      return trustStorePath;
    }

    public String getKeyStorePath() {
      return keyStorePath;
    }

    public String getKeyStoreType() {
      return keyStoreType;
    }

    public String getTrustStoreType() {
      return trustStoreType;
    }

    public String getKeyStorePassword() {
      return keyStorePassword;
    }

    public String getTrustStorePassword() {
      return trustStorePassword;
    }

    public String getKeyManagerAlgorithm() {
      return keyManagerAlgorithm;
    }

    public String getTrustManagerAlgorithm() {
      return trustManagerAlgorithm;
    }

    public boolean isUseOpenSsl() {
      return useOpenSsl;
    }

    public boolean isValidateCertificates() {
      return validateCertificates;
    }

    public boolean isAllowGeneratingSelfSignedCertificate() {
      return allowGeneratingSelfSignedCertificate;
    }

    public String getSecureRandomAlgorithm() {
      return secureRandomAlgorithm;
    }

    public static Builder builder() {
      return new Builder();
    }

    private VeniceTlsConfiguration(Builder builder) {
      this.hostname = builder.hostname;
      this.sslProtocol = builder.sslProtocol;
      this.trustStorePath = builder.trustStorePath;
      this.keyStorePath = builder.keyStorePath;
      this.keyStoreType = builder.keyStoreType;
      this.trustStoreType = builder.trustStoreType;
      this.keyStorePassword = builder.keyStorePassword;
      this.trustStorePassword = builder.trustStorePassword;
      this.keyPassphrase = builder.keyPassphrase;
      this.keyManagerAlgorithm = builder.keyManagerAlgorithm;
      this.trustManagerAlgorithm = builder.trustStoreManagerAlgorithm;
      this.useOpenSsl = builder.useOpenSsl;
      this.validateCertificates = builder.validateCertificates;
      this.allowGeneratingSelfSignedCertificate = builder.allowGeneratingSelfSignedCertificate;
      this.secureRandomAlgorithm = builder.secureRandomAlgorithm;
    }

    // builder pattern
    public static class Builder {
      private String hostname;
      private String sslProtocol;
      private String trustStorePath;
      private String keyStorePath;
      private String keyStoreType;
      private String trustStoreType;
      private String keyStorePassword;
      private String trustStorePassword;
      private String keyPassphrase;
      private String keyManagerAlgorithm;
      private String trustStoreManagerAlgorithm;
      private String secureRandomAlgorithm;
      private boolean useOpenSsl;
      private boolean validateCertificates;
      private boolean allowGeneratingSelfSignedCertificate;

      public Builder setHostname(String hostname) {
        this.hostname = hostname;
        return this;
      }

      public Builder setSslProtocol(String sslProtocol) {
        this.sslProtocol = sslProtocol;
        return this;
      }

      public Builder setTrustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
      }

      public Builder setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
      }

      public Builder setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
        return this;
      }

      public Builder setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
        return this;
      }

      public Builder setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
      }

      public Builder setKeyPassphrase(String keyPassphrase) {
        this.keyPassphrase = keyPassphrase;
        return this;
      }

      public Builder setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
      }

      public Builder setKeyManagerAlgorithm(String keyManagerAlgorithm) {
        this.keyManagerAlgorithm = keyManagerAlgorithm;
        return this;
      }

      public Builder setUseOpenSsl(boolean useOpenSsl) {
        this.useOpenSsl = useOpenSsl;
        return this;
      }

      public Builder setValidateCertificates(boolean validateCertificates) {
        this.validateCertificates = validateCertificates;
        return this;
      }

      public Builder setAllowGeneratingSelfSignedCertificate(boolean allowGeneratingSelfSignedCertificate) {
        this.allowGeneratingSelfSignedCertificate = allowGeneratingSelfSignedCertificate;
        return this;
      }

      public Builder setSecureRandomAlgorithm(String secureRandomAlgorithm) {
        this.secureRandomAlgorithm = secureRandomAlgorithm;
        return this;
      }

      public Builder setTrustStoreManagerAlgorithm(String trustStoreManagerAlgorithm) {
        this.trustStoreManagerAlgorithm = trustStoreManagerAlgorithm;
        return this;
      }

      public VeniceTlsConfiguration build() {
        return new VeniceTlsConfiguration(this);
      }
    }
  }
}
