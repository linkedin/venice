package com.linkedin.venice.hadoop.ssl;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.hadoop.KafkaPushJob;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTempFileSSLConfigurator {
  @Test
  public void testWriteToTempFile()
      throws IOException {
    String testCertStr = "test123";
    byte[] testCert = testCertStr.getBytes();
    TempFileSSLConfigurator configurator =
        (TempFileSSLConfigurator) SSLConfigurator.getSSLConfigurator(TempFileSSLConfigurator.class.getName());
    String path = configurator.writeToTempFile(testCert);
    File file = new File(path);
    try (FileInputStream fis = new FileInputStream(file)) {
      byte[] result = new byte[testCert.length];
      fis.read(result);
      Assert.assertTrue(Arrays.equals(testCert, result));
    }
  }

  @Test
  public void testGetCertification()
      throws IOException {
    String testCertStr = "test123";
    byte[] testCert = testCertStr.getBytes();
    Text text = new Text("testCert");
    Credentials credentials = new Credentials();
    credentials.addSecretKey(text, testCert);
    UserGroupInformation.getCurrentUser().addCredentials(credentials);

    TempFileSSLConfigurator configurator =
        (TempFileSSLConfigurator) SSLConfigurator.getSSLConfigurator(TempFileSSLConfigurator.class.getName());
    byte[] result = configurator.getCertification(UserGroupInformation.getCurrentUser().getCredentials(), text);
    Assert.assertTrue(Arrays.equals(testCert, result));
  }

  @Test
  public void testSetupSSLConfig()
      throws IOException {
    Properties properties = new Properties();
    properties.put(KafkaPushJob.SSL_KEY_STORE_PROPERTY_NAME, "linkedin.keystore");
    properties.put(KafkaPushJob.SSL_TRUST_STORE_PROPERTY_NAME, "linkedin.truststore");
    properties.put(KafkaPushJob.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "linkedin.keystorepassword");
    properties.put(KafkaPushJob.SSL_KEY_PASSWORD_PROPERTY_NAME, "linkedin.keypassword");
    properties.put(ConfigKeys.KAFKA_SECURITY_PROTOCOL, "ssl");

    String testCertStr = "test123";
    byte[] testCert = testCertStr.getBytes();
    String testPwd = "testpwd123";
    Credentials credentials = new Credentials();
    credentials.addSecretKey(new Text("linkedin.keystore"), testCert);
    credentials.addSecretKey(new Text("linkedin.truststore"), testCert);
    credentials.addSecretKey(new Text("linkedin.keystorepassword"), testPwd.getBytes(Charset.forName("UTF-8")));
    credentials.addSecretKey(new Text("linkedin.keypassword"), testPwd.getBytes(Charset.forName("UTF-8")));
    UserGroupInformation.getCurrentUser().addCredentials(credentials);

    TempFileSSLConfigurator configurator =
        (TempFileSSLConfigurator) SSLConfigurator.getSSLConfigurator(TempFileSSLConfigurator.class.getName());
    properties = configurator.setupSSLConfig(properties, UserGroupInformation.getCurrentUser().getCredentials());
    String[] paths = new String[]{properties.getProperty(ConfigKeys.SSL_KEYSTORE_LOCATION), properties.getProperty(
        ConfigKeys.SSL_TRUSTSTORE_LOCATION)};
    for (String path : paths) {
      File file = new File(path);
      try (FileInputStream fis = new FileInputStream(file)) {
        byte[] result = new byte[testCert.length];
        fis.read(result);
        Assert.assertTrue(Arrays.equals(testCert, result));
      }
    }
    Assert.assertEquals(testPwd, properties.get(ConfigKeys.SSL_KEY_PASSWORD));
    Assert.assertEquals(testPwd, properties.get(ConfigKeys.SSL_KEYSTORE_PASSWORD));
  }
}
