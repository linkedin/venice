package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.concurrency.Lazy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.base64.Base64Dialect;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 4/19/18.
 */
public final class SSLContextBuilder {
  private static final Logger LOG = LogManager.getLogger(SSLContextBuilder.class);

  private static final String DEFAULT_ALGORITHM = "SunX509";
  private static final String DEFAULT_PROTOCOL = "TLSv1.2";
  private static final String JKS_STORE_TYPE_NAME = "JKS";
  private static final String P12_STORE_TYPE_NAME = "PKCS12";

  // Permit clients to connect with TLSv1.1
  private static final String[] CLIENT_PROTOCOLS = { DEFAULT_PROTOCOL, "TLSv1.1", "TLSv1" };

  /**
   * Similiar ciphers as WITH_GCM_CIPHERS except with CBC substituted instead of GCM
   * because GCM is crushingly slow in current Java implementations.
   */
  public static final List<String> NO_GCM_CIPHERS = Collections.unmodifiableList(
      Arrays.asList(
          /* openssl = ECDHE-ECDSA-AES256-GCM-SHA384 */
          "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
          /* openssl = ECDHE-RSA-AES256-GCM-SHA384 */
          "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
          /* openssl = ECDHE-ECDSA-CHACHA20-POLY1305 */
          "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
          /* openssl = ECDHE-RSA-CHACHA20-POLY1305 */
          "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
          /* openssl = ECDHE-ECDSA-AES128-GCM-SHA256 */
          "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",

          /* REQUIRED BY HTTP/2 SPEC */
          /* openssl = ECDHE-RSA-AES128-GCM-SHA256 */
          "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
          /* REQUIRED BY HTTP/2 SPEC */

          // TLS 1.2 SHA2 family
          "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
          "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",

          "TLS_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_RSA_WITH_AES_128_CBC_SHA"));

  /**
   * Modern ciphers as used for HTTP/2 plus three weak ciphers as used within LNKD HTTP/1
   */
  public static final List<String> WITH_GCM_CIPHERS = Collections.unmodifiableList(
      Stream.concat(
          Http2SecurityUtil.CIPHERS.stream(),
          Stream
              .of("TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_GCM_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA"))
          .collect(Collectors.toList()));

  public static final List<String> CIPHERS =
      Boolean.parseBoolean(System.getProperty("com.linkedin.alpini.netty4.http2.useGcmCiphers", "false"))
          ? WITH_GCM_CIPHERS
          : NO_GCM_CIPHERS;

  private SSLContextBuilder() {
  }

  /**
   * Avoid a hard linkage to the OpenSsl class so that if the tc-native library is missing, we
   * can correctly return {@literal false}.
   * @param methodName name of static method of {@linkplain io.netty.handler.ssl.OpenSsl} class.
   * @return result of method or {@literal false}
   */
  private static boolean checkOpenSsl(String methodName) {
    try {
      return Boolean.TRUE
          .equals(Class.forName("io.netty.handler.ssl.OpenSsl").getDeclaredMethod(methodName).invoke(null));
    } catch (Throwable ex) {
      return false;
    }
  }

  public static boolean useOpenSsl() {
    return Boolean.parseBoolean(System.getProperty("com.linkedin.alpini.netty4.http2.useOpenSsl", "true"))
        && checkOpenSsl("isAvailable");
  }

  private static SslProvider getProvider(Provider sslContextProvider, boolean useRefCnt) {
    return sslContextProvider == null && useOpenSsl()
        ? (useRefCnt ? SslProvider.OPENSSL_REFCNT : SslProvider.OPENSSL)
        : SslProvider.JDK;
  }

  public static List<String> getCiphers(Provider sslContextProvider, SslProvider provider) {
    return sslContextProvider == null && provider == SslProvider.JDK ? CIPHERS : WITH_GCM_CIPHERS;
  }

  public static SslContext build(SslContextBuilder builder) throws SSLException {
    try {
      builder.build().newHandler(UnpooledByteBufAllocator.DEFAULT);
    } catch (Throwable ex) {
      LOG.error("ALPN not available", ex);
      builder.applicationProtocolConfig(null);
    }
    return builder.build();
  }

  private static ApplicationProtocolConfig makeApplicationProtocolConfig(boolean announceHttp2) {

    if (useOpenSsl() && !checkOpenSsl("isAlpnSupported")) {
      LOG.warn("Unable to configure ALPN with OpenSSL because it is not supported");
      return null;
    }

    String[] protocols = announceHttp2
        ? new String[] { ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1 }
        : new String[] { ApplicationProtocolNames.HTTP_1_1 };

    return new ApplicationProtocolConfig(
        ApplicationProtocolConfig.Protocol.ALPN,
        // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers
        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
        // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
        protocols);

  }

  /**
   *
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @return client SslContext
   * @throws SSLException
   */
  public static SslContext makeClientContext(long sessionCacheSize, long sessionTimeout) throws SSLException {
    return makeClientContext(sessionCacheSize, sessionTimeout, true);
  }

  /**
   *
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @param permitHttp2 permits HTTP/2 APLN negotiation when {@code true}.
   * @return client SslContext
   * @throws SSLException
   */
  public static SslContext makeClientContext(long sessionCacheSize, long sessionTimeout, boolean permitHttp2)
      throws SSLException {
    return makeClientContext(null, sessionCacheSize, sessionTimeout, permitHttp2);
  }

  public static SslContext makeClientContext(
      Provider sslContextProvider,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2) throws SSLException {
    return makeClientContext(sslContextProvider, sessionCacheSize, sessionTimeout, permitHttp2, false);
  }

  public static SslContext makeClientContext(
      Provider sslContextProvider,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2,
      boolean useRefCnt) throws SSLException {
    SslProvider provider = getProvider(sslContextProvider, useRefCnt);
    return build(
        SslContextBuilder.forClient()
            .sslProvider(provider)
            .sslContextProvider(sslContextProvider)
            .protocols(DEFAULT_PROTOCOL)
            .ciphers(getCiphers(sslContextProvider, provider), SupportedCipherSuiteFilter.INSTANCE)
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .sessionCacheSize(sessionCacheSize)
            .sessionTimeout(sessionTimeout)
            .applicationProtocolConfig(makeApplicationProtocolConfig(permitHttp2)));
  }

  /**
   *
   * @param keyStoreFile keystore file
   * @param keyStorePassword keystore password
   * @param keyStoreType keystore type
   * @param trustStoreFile truststore file
   * @param trustStorePassword truststore password
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @return client SslContext
   * @throws IOException
   * @throws KeyStoreException
   * @throws UnrecoverableKeyException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   */
  public static SslContext makeClientContext(
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return makeClientContext(
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        true);
  }

  /**
   *
   * @param keyStoreFile keystore file
   * @param keyStorePassword keystore password
   * @param keyStoreType keystore type
   * @param trustStoreFile truststore file
   * @param trustStorePassword truststore password
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @param permitHttp2 permits HTTP/2 APLN negotiation when {@code true}.
   * @return client SslContext
   * @throws IOException
   * @throws KeyStoreException
   * @throws UnrecoverableKeyException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   */
  public static SslContext makeClientContext(
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return makeClientContext(
        null,
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        permitHttp2);
  }

  public static SslContext makeClientContext(
      Provider sslContextProvider,
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return makeClientContext(
        sslContextProvider,
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        permitHttp2,
        false);
  }

  public static SslContext makeClientContext(
      Provider sslContextProvider,
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2,
      boolean useRefCnt)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return build(
        setupContext(
            sslContextProvider,
            keyStoreFile,
            keyStorePassword,
            keyStoreType,
            trustStoreFile,
            trustStorePassword,
            sessionCacheSize,
            sessionTimeout,
            permitHttp2,
            useRefCnt).apply(kmf -> SslContextBuilder.forClient().keyManager(kmf)));
  }

  private static final Supplier<SelfSignedCertificate> SELF_SIGNED_CERTIFICATE_SUPPLER =
      Lazy.of(SSLContextBuilder::constructSelfSignedCertificate);

  private static SelfSignedCertificate constructSelfSignedCertificate() {
    try {
      return new SelfSignedCertificate();
    } catch (CertificateException e) {
      throw new Error(e);
    }
  }

  /**
   *
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @return server SslContext
   * @throws SSLException
   */
  public static SslContext makeServerContext(long sessionCacheSize, long sessionTimeout) throws SSLException {
    return makeServerContext(sessionCacheSize, sessionTimeout, true);
  }

  /**
   *
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @param permitHttp2 permits HTTP/2 APLN negotiation when {@code true}.
   * @return server SslContext
   * @throws SSLException
   */
  public static SslContext makeServerContext(long sessionCacheSize, long sessionTimeout, boolean permitHttp2)
      throws SSLException {
    return makeServerContext(null, sessionCacheSize, sessionTimeout, permitHttp2);
  }

  public static SslContext makeServerContext(
      Provider sslContextProvider,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2) throws SSLException {
    return makeServerContext(sslContextProvider, sessionCacheSize, sessionTimeout, permitHttp2, false);
  }

  public static SslContext makeServerContext(
      Provider sslContextProvider,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2,
      boolean useRefCnt) throws SSLException {
    SslProvider provider = getProvider(sslContextProvider, useRefCnt);
    SelfSignedCertificate ssc = SELF_SIGNED_CERTIFICATE_SUPPLER.get();
    return build(
        SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
            .sslProvider(provider)
            .sslContextProvider(sslContextProvider)
            .protocols(CLIENT_PROTOCOLS)
            .ciphers(getCiphers(sslContextProvider, provider), SupportedCipherSuiteFilter.INSTANCE)
            .sessionCacheSize(sessionCacheSize)
            .sessionTimeout(sessionTimeout)
            .applicationProtocolConfig(makeApplicationProtocolConfig(permitHttp2)));
  }

  /**
   *
   * @param keyStoreFile keystore file
   * @param keyStorePassword keystore password
   * @param keyStoreType keystore type
   * @param trustStoreFile truststore file
   * @param trustStorePassword truststore password
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @return server SslContext
   * @throws IOException
   * @throws KeyStoreException
   * @throws UnrecoverableKeyException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   */
  public static SslContext makeServerContext(
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout)
      throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
    return makeServerContext(
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        true);
  }

  /**
   *
   * @param keyStoreFile keystore file
   * @param keyStorePassword keystore password
   * @param keyStoreType keystore type
   * @param trustStoreFile truststore file
   * @param trustStorePassword truststore password
   * @param sessionCacheSize size of the cache used for storing SSL session objects. {@code 0} to use the
   *                         default value.
   * @param sessionTimeout timeout for the cached SSL session objects, in seconds. {@code 0} to use the
   *                       default value.
   * @param permitHttp2 permits HTTP/2 APLN negotiation when {@code true}.
   * @return server SslContext
   * @throws IOException
   * @throws KeyStoreException
   * @throws UnrecoverableKeyException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   */
  public static SslContext makeServerContext(
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return makeServerContext(
        null,
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        permitHttp2);
  }

  public static SslContext makeServerContext(
      Provider sslContextProvider,
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return makeServerContext(
        sslContextProvider,
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        permitHttp2,
        false);
  }

  public static SslContext makeServerContext(
      Provider sslContextProvider,
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2,
      boolean useRefCnt)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return build(
        setupContext(
            sslContextProvider,
            keyStoreFile,
            keyStorePassword,
            keyStoreType,
            trustStoreFile,
            trustStorePassword,
            sessionCacheSize,
            sessionTimeout,
            permitHttp2,
            useRefCnt).apply(SslContextBuilder::forServer));
  }

  public static Function<Function<KeyManagerFactory, SslContextBuilder>, SslContextBuilder> setupContext(
      Provider sslContextProvider,
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
    return setupContext(
        sslContextProvider,
        keyStoreFile,
        keyStorePassword,
        keyStoreType,
        trustStoreFile,
        trustStorePassword,
        sessionCacheSize,
        sessionTimeout,
        permitHttp2,
        false);
  }

  public static Function<Function<KeyManagerFactory, SslContextBuilder>, SslContextBuilder> setupContext(
      Provider sslContextProvider,
      Object keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStorePassword,
      long sessionCacheSize,
      long sessionTimeout,
      boolean permitHttp2,
      boolean useRefCnt)
      throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {

    SslProvider provider = getProvider(sslContextProvider, useRefCnt);

    if (!keyStoreType.equalsIgnoreCase(P12_STORE_TYPE_NAME) && !keyStoreType.equalsIgnoreCase(JKS_STORE_TYPE_NAME)) {
      throw new NoSuchAlgorithmException("Unsupported keyStoreType: " + keyStoreType);
    }

    // Load the key Store
    final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
    keyStore.load(toInputStream(keyStoreFile), keyStorePassword.toCharArray());

    // Load trust store
    final KeyStore trustStore;
    if (trustStoreFile != null) {
      trustStore = KeyStore.getInstance(JKS_STORE_TYPE_NAME);
      trustStore.load(toInputStream(trustStoreFile), trustStorePassword.toCharArray());
    } else {
      trustStore = keyStore;
    }

    // Set key manager from key store
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
    kmf.init(keyStore, keyStorePassword.toCharArray());

    // Set trust manager from trust store
    final TrustManagerFactory trustFact = TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
    trustFact.init(trustStore);

    LOG.info("setupContext provider={} sslContextProvider={}", provider, sslContextProvider);

    return builder -> builder.apply(kmf)
        .sslProvider(provider)
        .sslContextProvider(sslContextProvider)
        .protocols(CLIENT_PROTOCOLS)
        .ciphers(getCiphers(sslContextProvider, provider), SupportedCipherSuiteFilter.INSTANCE)
        .trustManager(trustFact)
        .sessionCacheSize(sessionCacheSize)
        .sessionTimeout(sessionTimeout)
        .applicationProtocolConfig(makeApplicationProtocolConfig(permitHttp2));

  }

  private static InputStream toInputStream(Object store) throws IOException {
    if (store instanceof File) {
      File storeFile = (File) store;
      int length = Math.toIntExact(storeFile.length());
      ByteBuf buf = Unpooled.buffer(length);
      try (InputStream is = new FileInputStream(storeFile)) {
        buf.writeBytes(is, length);
      }
      return new ByteBufInputStream(buf);
    } else {
      ByteBuf msg = Unpooled.copiedBuffer(store.toString(), StandardCharsets.US_ASCII);
      return new ByteBufInputStream(Base64.decode(msg, msg.readerIndex(), msg.readableBytes(), Base64Dialect.STANDARD));
    }
  }

}
