package com.linkedin.venice.hadoop.ssl;

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
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TempFileSSLConfigurator implements SSLConfigurator {
  private static final Logger LOGGER = LogManager.getLogger(TempFileSSLConfigurator.class);

  @Override
  public Properties setupSSLConfig(Properties props, Credentials userCredentials) {
    Properties properties = new Properties();
    properties.putAll(props);
    if (properties.containsKey(KAFKA_SECURITY_PROTOCOL)
        && properties.getProperty(KAFKA_SECURITY_PROTOCOL).toLowerCase().equals("ssl")) {
      LOGGER.info("Start setting up the ssl properties.");
      try {
        // Setup keystore certification
        byte[] keyStoreCert =
            getCertification(userCredentials, new Text(properties.getProperty(SSL_KEY_STORE_PROPERTY_NAME)));
        LOGGER.info("Found key store cert from credentials.");
        String keyStoreLocation = writeToTempFile(keyStoreCert);
        LOGGER.info("Write key store cert to file: {}", keyStoreLocation);
        properties.put(SSL_KEYSTORE_LOCATION, keyStoreLocation);
        // Setup truststore certification
        byte[] truestStoreCert =
            getCertification(userCredentials, new Text(properties.getProperty(SSL_TRUST_STORE_PROPERTY_NAME)));
        LOGGER.info("Found trust store cert from credentials.");
        String trustStoreLocation = writeToTempFile(truestStoreCert);
        LOGGER.info("Write trust store cert to file: {}", trustStoreLocation);
        properties.put(SSL_TRUSTSTORE_LOCATION, trustStoreLocation);

        // Setup keystore password.
        String keyStorePassword =
            getPassword(userCredentials, new Text(properties.getProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME)));
        properties.put(SSL_KEYSTORE_PASSWORD, keyStorePassword);

        // Setup key password.
        String keyPassword =
            getPassword(userCredentials, new Text(properties.getProperty(SSL_KEY_PASSWORD_PROPERTY_NAME)));
        properties.put(SSL_KEY_PASSWORD, keyPassword);
        if (!properties.containsKey(SSL_KEYSTORE_TYPE)) {
          properties.put(SSL_KEYSTORE_TYPE, "pkcs12");
        }
        if (!properties.containsKey(SSL_TRUSTSTORE_TYPE)) {
          properties.put(SSL_TRUSTSTORE_TYPE, "JKS");
        }
        if (!properties.containsKey(SSL_TRUSTSTORE_PASSWORD)) {
          properties.put(SSL_TRUSTSTORE_PASSWORD, "changeit");
        }
        if (!properties.containsKey(SSL_TRUSTMANAGER_ALGORITHM)) {
          properties.put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
        }
        if (!properties.containsKey(SSL_KEYMANAGER_ALGORITHM)) {
          properties.put(SSL_KEYMANAGER_ALGORITHM, "SunX509");
        }
        if (!properties.containsKey(SSL_SECURE_RANDOM_IMPLEMENTATION)) {
          properties.put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
        }
        LOGGER.info("Complete setting up the ssl properties.");
      } catch (VeniceException e) {
        throw e;
      }
    }

    return properties;
  }

  protected String getPassword(Credentials credentials, Text propertyName) {
    return new String(credentials.getSecretKey(propertyName), StandardCharsets.UTF_8);
  }

  protected byte[] getCertification(Credentials credentials, Text certName) {
    byte[] cert = credentials.getSecretKey(certName);
    if (cert == null) {
      throw new VeniceException("Could not find certification: " + certName.toString() + " under user's credentials.");
    }
    return cert;
  }

  protected String writeToTempFile(byte[] certification) {
    File file;
    try {
      Path path = Files.createTempFile(null, null);
      IOUtils.copyBytes(
          new ByteArrayInputStream(certification),
          Files.newOutputStream(path),
          (long) certification.length,
          true);
      file = path.toFile();
      // Clean up the cert temp file once job completed.
      // If jvm is terminated unexpectedly, the file will be left, but as it's the tmp file so it should be collected
      // eventually.
      file.deleteOnExit();
      if (certification.length != file.length() || !file.setReadable(true, true)) {
        throw new VeniceException("Unable to create or chmod file: " + file.getAbsolutePath());
      }
      return file.getCanonicalPath();
    } catch (IOException e) {
      throw new VeniceException("Unable to create temp file for certification.", e);
    }
  }
}
