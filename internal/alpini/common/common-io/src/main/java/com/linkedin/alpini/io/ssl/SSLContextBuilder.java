package com.linkedin.alpini.io.ssl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Base64;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;


/**
 * Builds a new instance of {@link SSLContext} object from given key store and trust store parameters.
 */
public class SSLContextBuilder {
  private static final String DEFAULT_ALGORITHM = "SunX509";
  private static final String DEFAULT_PROTOCOL = "TLS";
  private static final String JKS_STORE_TYPE_NAME = "JKS";
  private static final String P12_STORE_TYPE_NAME = "PKCS12";

  /**
   * This constructor uses keyStoreData for both keyStore and trustStore.
   * It only supports base64 encoded jks format keyStore.
   *
   * @deprecated Builds from key store file instead of raw key store data.
   */
  @Deprecated
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
    final SSLContext secureContext = SSLContext.getInstance(DEFAULT_PROTOCOL);
    secureContext.init(kmf.getKeyManagers(), trustFact.getTrustManagers(), null);
    return secureContext;
  }

  /**
   * The keyStoreFile takes a File object of p12 or jks file depends on keyStoreType
   * The trustStoreFile always takes a File object of JKS file.
   */
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
    final SSLContext secureContext = SSLContext.getInstance(DEFAULT_PROTOCOL);
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
