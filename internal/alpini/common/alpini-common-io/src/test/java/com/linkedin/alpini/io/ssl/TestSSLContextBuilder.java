package com.linkedin.alpini.io.ssl;

import java.io.File;
import javax.net.ssl.SSLContext;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSSLContextBuilder {
  private static final String jksKeyStoreFilePath = "src/test/resources/identity.jks";
  private static final String jksKeyStoreFilePassword = "clientpassword";
  private static final String pkcs12KeyStoreFilePath = "src/test/resources/identity.p12";
  private static final String pkcs12KeyStoreFilePassword = "work_around_jdk-6879539";
  private static final String jksTrustStoreFilePath = "src/test/resources/trustStore.jks";
  private static final String jksTrustStoreFilePassword = "work_around_jdk-6879539";

  private static final String incorrectFilePath = "src/test/resources/none";
  private static final String incorrectPassword = "wrongpassword";

  @Test(groups = "unit")
  public void testSslContextConstructorForJKS() {
    testSslContextConstructor(
        new File(jksKeyStoreFilePath),
        jksKeyStoreFilePassword,
        "JKS",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ false);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForPKCS12() {
    testSslContextConstructor(
        new File(pkcs12KeyStoreFilePath),
        pkcs12KeyStoreFilePassword,
        "PKCS12",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ false);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForIncorrectStoreType() {
    testSslContextConstructor(
        new File(pkcs12KeyStoreFilePath),
        pkcs12KeyStoreFilePassword,
        "PKCS",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ true);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForJKSwithIncorrectKeyStore() {
    testSslContextConstructor(
        new File(incorrectFilePath),
        jksKeyStoreFilePassword,
        "JKS",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ true);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForPKCS12withIncorrectKeyStore() {
    testSslContextConstructor(
        new File(incorrectFilePath),
        jksKeyStoreFilePassword,
        "PKCS12",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ true);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForJSKwithIncorrectPassword() {
    testSslContextConstructor(
        new File(jksKeyStoreFilePath),
        incorrectPassword,
        "JKS",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ true);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForPKCS12withIncorrectPassword() {
    testSslContextConstructor(
        new File(pkcs12KeyStoreFilePath),
        incorrectPassword,
        "PKCS12",
        new File(jksTrustStoreFilePath),
        jksTrustStoreFilePassword,
        /* shouldThrowException = */ true);
  }

  @Test(groups = "unit")
  public void testSslContextConstructorForIncorrectTrustStorePassowrd() {
    testSslContextConstructor(
        new File(pkcs12KeyStoreFilePath),
        pkcs12KeyStoreFilePassword,
        "PKCS12",
        new File(jksTrustStoreFilePath),
        incorrectPassword,
        /* shouldThrowException = */ true);
  }

  private void testSslContextConstructor(
      File keyStoreFile,
      String keyStorePassword,
      String keyStoreType,
      File trustStoreFile,
      String trustStoreFilePassword,
      boolean shouldThrowException) {
    SSLContext context = null;
    try {
      context = new SSLContextBuilder()
          .build(keyStoreFile, keyStorePassword, keyStoreType, trustStoreFile, trustStoreFilePassword);

      if (shouldThrowException) {
        Assert.fail("Exception not thrown");
      }

      Assert.assertNotNull(context);
    } catch (Exception e) {
      if (!shouldThrowException) {
        Assert.fail("Fail to create SSLContext: ", e);
      }
    }
  }
}
