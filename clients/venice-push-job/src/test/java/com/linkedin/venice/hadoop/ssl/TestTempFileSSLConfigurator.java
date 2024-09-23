package com.linkedin.venice.hadoop.ssl;

import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;

import com.linkedin.venice.CommonConfigKeys;
import com.linkedin.venice.ConfigKeys;
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
  public void testWriteToTempFile() throws IOException {
    String testCertStr = "test123";
    byte[] testCert = testCertStr.getBytes();
    TempFileSSLConfigurator configurator =
        (TempFileSSLConfigurator) SSLConfigurator.getSSLConfigurator(TempFileSSLConfigurator.class.getName());
    String path = configurator.writeToTempFile(testCert);
    File file = new File(path);
    try (FileInputStream fis = new FileInputStream(file)) {
      byte[] result = new byte[testCert.length];
      int length = fis.read(result);
      Assert.assertEquals(length, testCert.length);
      Assert.assertTrue(Arrays.equals(testCert, result));
    }
  }

  @Test
  public void testGetCertification() throws IOException {
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
  public void testSetupSSLConfig() throws IOException {
    Properties properties = new Properties();
    properties.put(SSL_KEY_STORE_PROPERTY_NAME, "linkedin.keystore");
    properties.put(SSL_TRUST_STORE_PROPERTY_NAME, "linkedin.truststore");
    properties.put(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "linkedin.keystorepassword");
    properties.put(SSL_KEY_PASSWORD_PROPERTY_NAME, "linkedin.keypassword");
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
    String[] paths = new String[] { properties.getProperty(CommonConfigKeys.SSL_KEYSTORE_LOCATION),
        properties.getProperty(CommonConfigKeys.SSL_TRUSTSTORE_LOCATION) };
    for (String path: paths) {
      File file = new File(path);
      try (FileInputStream fis = new FileInputStream(file)) {
        byte[] result = new byte[testCert.length];
        int length = fis.read(result);
        Assert.assertEquals(length, testCert.length);
        Assert.assertTrue(Arrays.equals(testCert, result));
      }
    }
    Assert.assertEquals(testPwd, properties.get(CommonConfigKeys.SSL_KEY_PASSWORD));
    Assert.assertEquals(testPwd, properties.get(CommonConfigKeys.SSL_KEYSTORE_PASSWORD));
  }
}
