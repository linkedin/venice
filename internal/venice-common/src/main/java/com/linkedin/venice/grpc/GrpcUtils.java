package com.linkedin.venice.grpc;

import com.google.protobuf.ByteString;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.grpc.Grpc;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class GrpcUtils {
  private static final Logger LOGGER = LogManager.getLogger(GrpcUtils.class);

  public static KeyManager[] getKeyManagers(SSLFactory sslFactory)
      throws UnrecoverableKeyException, CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    String algorithm = KeyManagerFactory.getDefaultAlgorithm();
    String password = sslFactory.getSSLConfig().getKeyStorePassword();
    KeyStore keyStore = loadKeyStore(sslFactory, sslFactory.getSSLConfig().getKeyStoreType());
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
    keyManagerFactory.init(keyStore, password.toCharArray());
    return keyManagerFactory.getKeyManagers();
  }

  public static TrustManager[] getTrustManagers(SSLFactory sslFactory)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    String algorithm = TrustManagerFactory.getDefaultAlgorithm();
    KeyStore trustStore = loadTrustStore(sslFactory, sslFactory.getSSLConfig().getTrustStoreType());
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
    trustManagerFactory.init(trustStore);
    return trustManagerFactory.getTrustManagers();
  }

  public static Status httpResponseStatusToGrpcStatus(HttpResponseStatus status, String errorMessage) {
    if (status.equals(HttpResponseStatus.FORBIDDEN) || status.equals(HttpResponseStatus.UNAUTHORIZED)) {
      return Status.PERMISSION_DENIED.withDescription(errorMessage);
    }

    return Status.UNKNOWN.withDescription(errorMessage);
  }

  public static X509Certificate extractGrpcClientCert(ServerCall<?, ?> call) throws SSLPeerUnverifiedException {
    SSLSession sslSession = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
    if (sslSession == null) {
      LOGGER.error("Cannot obtain SSLSession for authority {}", call.getAuthority());
      throw new VeniceException("Failed to obtain SSL session");
    }

    return SslUtils.getX509Certificate(sslSession.getPeerCertificates()[0]);
  }

  private static KeyStore loadKeyStore(SSLFactory sslFactory, String type)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    SSLConfig config = sslFactory.getSSLConfig();
    String path = config.getKeyStoreFilePath();
    String password = config.getKeyStorePassword();
    return loadStore(path, password.toCharArray(), type);
  }

  private static KeyStore loadTrustStore(SSLFactory sslFactory, String type)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    SSLConfig config = sslFactory.getSSLConfig();
    String path = config.getTrustStoreFilePath();
    String password = config.getTrustStoreFilePassword();
    return loadStore(path, password.toCharArray(), type);
  }

  private static KeyStore loadStore(String path, char[] password, String type)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
    KeyStore keyStore = KeyStore.getInstance(type);
    try (InputStream in = Files.newInputStream(Paths.get(path))) {
      keyStore.load(in, password);
    }
    return keyStore;
  }

  /**
   * Converts a Netty ByteBuf to a ByteString, checking if it has a backing array to avoid manual copying.
   *
   * @param body The ByteBuf to be converted to ByteString.
   * @return The resulting ByteString.
   */
  public static ByteString toByteString(ByteBuf body) {
    if (body.hasArray()) {
      // Directly use the backing array to avoid copying
      return ByteString.copyFrom(body.array(), body.arrayOffset() + body.readerIndex(), body.readableBytes());
    }
    // Fallback to nioBuffer() to handle the conversion efficiently
    return ByteString.copyFrom(body.nioBuffer());
  }

  public static ByteString toByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }
}
