package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.netty4.http2.SSLContextBuilder;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;


public class SSLEngineFactoryImpl implements SSLEngineFactory {
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

  private static final File NULL_FILE = new File("/dev/null");

  private SslContext _serverContext;
  private SslContext _clientContext;

  public SSLEngineFactoryImpl(Config config) throws Exception {
    _sslEnabled = config.getSslEnabled();
    if (_sslEnabled) {
      _keyStoreFilePath = config.getKeyStoreFilePath();
      _trustStoreFilePath = config.getTrustStoreFilePath();
      _keyStoreData = config.getKeyStoreData();

      Provider sslContextProvider = config.getSslContextProvider();

      // Either keyStoreFilePath + trustStoreFilePath or keyStoreData must be provided.
      final LegacyBuilder sslContextBuilder = new LegacyBuilder(sslContextProvider);
      if (StringUtils.isNotBlank(_keyStoreData)) {
        _context = new SSLContextFactory(_keyStoreData, config.getKeyStorePassword()).getContext();
        _context = sslContextBuilder.build(getKeyStoreData(), config.getKeyStorePassword());
      } else if (StringUtils.isNotBlank(_keyStoreFilePath) && StringUtils.isNotBlank(_trustStoreFilePath)) {
        _context = new SSLContextFactory(
            new File(_keyStoreFilePath),
            config.getKeyStorePassword(),
            config.getKeyStoreType(),
            new File(_trustStoreFilePath),
            config.getTrustStoreFilePassword()).getContext();
        _context = sslContextBuilder.build(
            new File(getKeyStoreFilePath()),
            config.getKeyStorePassword(),
            config.getKeyStoreType(),
            new File(getTrustStoreFilePath()),
            config.getTrustStoreFilePassword());
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

      Object keyStoreData = Optional.ofNullable(config.getKeyStoreData())
          .filter(((Predicate<String>) String::isEmpty).negate())
          .orElse(null);

      Function<Function<KeyManagerFactory, SslContextBuilder>, SslContextBuilder> setup =
          SSLContextBuilder.setupContext(
              sslContextProvider,
              keyStoreData != null ? keyStoreData : new File(getKeyStoreFilePath()),
              config.getKeyStorePassword(),
              config.getKeyStoreType(),
              keyStoreData != null ? null : new File(getTrustStoreFilePath()),
              config.getTrustStoreFilePassword(),
              config.getSessionCacheSize(),
              config.getSessionTimeout(),
              config.isPermitHttp2(),
              config.isUseRefCount());

      // As per https://blogs.oracle.com/java/post/diagnosing-tls-ssl-and-https
      // we cannot just specify "TLS" because modern java will default to TLSv1.2
      // so we must force to TLSv1 which was the default in Java 7 and earlier.
      Function<SslContextBuilder, SslContextBuilder> securityAdjustment =
          config.getUseInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk()
              ? builder -> builder.protocols("TLSv1")
              : Function.identity();

      _serverContext = SSLContextBuilder.build(securityAdjustment.apply(setup.apply(SslContextBuilder::forServer)));
      _clientContext =
          SSLContextBuilder.build(securityAdjustment.apply(setup.apply(SslContextBuilder.forClient()::keyManager)));

      if (sslContextProvider != null && !config.getUseInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk()) {
        // Add back in the Http2SecurityUtil.CIPHERS as preferred if we're using a SSLContext provider
        // but filter out any unsupported cipher suite.
        Set<String> supportedCipherSuiteSet =
            new HashSet<>(Arrays.asList(getSSLContext().getSocketFactory().getSupportedCipherSuites()));
        getSSLParameters().setCipherSuites(
            Stream
                .concat(
                    Http2SecurityUtil.CIPHERS.stream().filter(supportedCipherSuiteSet::contains),
                    Stream.of(getSSLParameters().getCipherSuites()))
                .distinct()
                .toArray(String[]::new));
      }
    } else {
      _context = null;
      _parameters = null;
    }
  }

  public static String[] filterDisallowedCiphersuites(String[] ciphersuites) throws SSLProtocolException {
    Set<String> allowedCiphers = new HashSet<>();
    Collections.addAll(allowedCiphers, CIPHER_SUITE_ALLOWLIST);

    Set<String> supportedCiphers = new HashSet<>();
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
    private Provider _sslContextProvider;
    private boolean _permitHttp2 = true;
    private long _sessionCacheSize = 0L;
    private long _sessionTimeout = 0L;
    private boolean _useInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk;
    private boolean _useRefCount;

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

    public boolean isPermitHttp2() {
      return _permitHttp2 && !_useInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk;
    }

    public void setPermitHttp2(boolean permitHttp2) {
      _permitHttp2 = permitHttp2;
    }

    public long getSessionCacheSize() {
      return _sessionCacheSize;
    }

    public void setSessionCacheSize(long sessionCacheSize) {
      _sessionCacheSize = sessionCacheSize;
    }

    public long getSessionTimeout() {
      return _sessionTimeout;
    }

    public void setSessionTimeout(long sessionTimeout) {
      _sessionTimeout = sessionTimeout;
    }

    public boolean getUseInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk() {
      return _useInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk;
    }

    public void setUseInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk(
        boolean useInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk) {
      _useInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk =
          useInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk;
    }

    public Provider getSslContextProvider() {
      return _sslContextProvider;
    }

    public void setSslContextProvider(Provider sslContextProvider) {
      _sslContextProvider = sslContextProvider;
    }

    public boolean isUseRefCount() {
      return _useRefCount;
    }

    public void setUseRefCount(boolean useRefCount) {
      _useRefCount = useRefCount;
    }
  }

  private static class SSLContextFactory {
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
      if (!P12_STORE_TYPE_NAME.equalsIgnoreCase(keyStoreType) && !JKS_STORE_TYPE_NAME.equalsIgnoreCase(keyStoreType)) {
        throw new IllegalArgumentException("Unsupported keyStoreType: " + keyStoreType);
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

  @Override
  public SslContext context(boolean isServer) {
    return isServer ? _serverContext : _clientContext;
  }

  @Override
  public SSLEngine createSSLEngine(ByteBufAllocator alloc, String host, int port, boolean isServer) {
    return init(context(isServer).newEngine(alloc, host, port), isServer);
  }

  @Override
  public SSLEngine createSSLEngine(ByteBufAllocator alloc, boolean isServer) {
    return init(context(isServer).newEngine(alloc), isServer);
  }

  private SSLEngine init(SSLEngine engine, boolean isServer) {
    if (isServer && isSslRequireClientCerts()) {
      engine.setNeedClientAuth(true);
    }
    return engine;
  }

  @Override
  public SSLSessionContext sessionContext(boolean isServer) {
    return context(isServer).sessionContext();
  }

  private String getKeyStoreFilePath() {
    return _keyStoreFilePath;
  }

  private String getTrustStoreFilePath() {
    return _trustStoreFilePath;
  }

  private String getKeyStoreData() {
    return _keyStoreData;
  }

  private static SSLContext getInstance(String protocol, Provider provider) throws NoSuchAlgorithmException {
    return provider == null ? SSLContext.getInstance(protocol) : SSLContext.getInstance(protocol, provider);
  }

  private static class LegacyBuilder extends com.linkedin.alpini.io.ssl.SSLContextBuilder {
    private static final String DEFAULT_ALGORITHM = "SunX509";
    private static final String DEFAULT_PROTOCOL = "TLS";
    private static final String JKS_STORE_TYPE_NAME = "JKS";
    private static final String P12_STORE_TYPE_NAME = "PKCS12";

    private final Provider _provider;

    LegacyBuilder(Provider sslContextProvider) {
      _provider = sslContextProvider;
    }

    @Override
    public SSLContext build(String keyStoreData, String keyStorePassword) throws Exception {
      // Load the key store
      final KeyStore certKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      certKeyStore.load(toInputStream(keyStoreData), keyStorePassword.toCharArray());

      // Set key manager toInputStream() use X509
      final KeyManagerFactory kmf = KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
      kmf.init(certKeyStore, keyStorePassword.toCharArray());

      // Use a standard trust manager
      final TrustManagerFactory trustFact = TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
      trustFact.init(certKeyStore);

      // Set context to TLS and initialize it
      final SSLContext secureContext = getInstance(DEFAULT_PROTOCOL, _provider);
      secureContext.init(kmf.getKeyManagers(), trustFact.getTrustManagers(), null);
      return secureContext;
    }

    @Override
    public SSLContext build(
        File keyStoreFile,
        String keyStorePassword,
        String keyStoreType,
        File trustStoreFile,
        String trustStorePassword) throws Exception {
      if (!P12_STORE_TYPE_NAME.equalsIgnoreCase(keyStoreType) && !JKS_STORE_TYPE_NAME.equalsIgnoreCase(keyStoreType)) {
        throw new IllegalArgumentException("Unsupported keyStoreType: " + keyStoreType);
      }

      // Load the key Store
      final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(toInputStream(keyStoreFile), keyStorePassword.toCharArray());

      // Load trust store
      final KeyStore trustStore = KeyStore.getInstance(JKS_STORE_TYPE_NAME);
      trustStore.load(toInputStream(trustStoreFile), trustStorePassword.toCharArray());

      // Set key manager from key store
      final KeyManagerFactory kmf = KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
      kmf.init(keyStore, keyStorePassword.toCharArray());

      // Set trust manager from trust store
      final TrustManagerFactory trustFact = TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
      trustFact.init(trustStore);

      // Set context to TLS and initialize it
      final SSLContext secureContext = getInstance(DEFAULT_PROTOCOL, _provider);
      secureContext.init(kmf.getKeyManagers(), trustFact.getTrustManagers(), null);
      return secureContext;
    }

    private InputStream toInputStream(String storeData) {
      byte[] data = Base64.getDecoder().decode(storeData);
      return new ByteArrayInputStream(data);
    }

    private InputStream toInputStream(File storeFile) throws IOException {
      byte[] data = FileUtils.readFileToByteArray(storeFile);
      return new ByteArrayInputStream(data);
    }
  }
}
