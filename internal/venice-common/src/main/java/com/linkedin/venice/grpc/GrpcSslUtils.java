package com.linkedin.venice.grpc;

import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;


public class GrpcSslUtils {
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

  public static KeyManager[] getKeyManagers(SSLFactory sslFactory)
      throws UnrecoverableKeyException, CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    String algorithm = KeyManagerFactory.getDefaultAlgorithm();
    return getKeyManagers(sslFactory, algorithm);
  }

  public static TrustManager[] getTrustManagers(SSLFactory sslFactory)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    String algorithm = TrustManagerFactory.getDefaultAlgorithm();
    return getTrustManagers(sslFactory, algorithm);
  }

  public static KeyManager[] getKeyManagers(SSLFactory sslFactory, String algorithm)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, UnrecoverableKeyException {
    String password = sslFactory.getSSLConfig().getKeyStorePassword();
    KeyStore keyStore = loadKeyStore(sslFactory, sslFactory.getSSLConfig().getKeyStoreType());
    return createKeyManagers(keyStore, algorithm, password);
  }

  public static TrustManager[] getTrustManagers(SSLFactory sslFactory, String algorithm)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    KeyStore trustStore = loadTrustStore(sslFactory, sslFactory.getSSLConfig().getKeyStoreType());
    return createTrustManagers(trustStore, algorithm);
  }

  private static KeyManager[] createKeyManagers(KeyStore keyStore, String algorithm, String password)
      throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
    keyManagerFactory.init(keyStore, password.toCharArray());
    return keyManagerFactory.getKeyManagers();
  }

  private static TrustManager[] createTrustManagers(KeyStore trustStore, String algorithm)
      throws NoSuchAlgorithmException, KeyStoreException {
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
    trustManagerFactory.init(trustStore);
    return trustManagerFactory.getTrustManagers();
  }

}
