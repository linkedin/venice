package com.linkedin.venice;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JDKCompatibilityTest {
  private static final int JAVA_8_CLASS_FILE_MAJOR_VERSION = 52;
  private static final Logger LOGGER = LogManager.getLogger(JDKCompatibilityTest.class);

  @Test
  public void testJavaRuntime() {
    String javaRuntime = System.getProperty("java.version");
    LOGGER.info("Java runtime: {}", javaRuntime);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    try {
      buffer.position(1);
    } catch (Throwable e) {
      Assert.fail("Test failed with Java runtime: " + javaRuntime, e);
    }
    Assert.assertEquals(buffer.get(), 2);
  }

  @Test
  public void testJava8BytecodeVersion() throws IOException {
    InputStream classBytes = JDKCompatibilityTest.class.getResourceAsStream("JDKCompatibilityTest.class");
    Assert.assertNotNull(classBytes, "The test class resource must be available");
    try (DataInputStream classFile = new DataInputStream(classBytes)) {
      Assert.assertEquals(classFile.readInt(), 0xCAFEBABE);
      classFile.readUnsignedShort();
      Assert.assertEquals(classFile.readUnsignedShort(), JAVA_8_CLASS_FILE_MAJOR_VERSION);
    }
  }
}
