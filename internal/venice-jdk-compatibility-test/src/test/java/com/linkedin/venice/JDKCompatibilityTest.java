package com.linkedin.venice;

import com.linkedin.venice.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JDKCompatibilityTest {
  private static final int JAVA_11_CLASS_FILE_MAJOR_VERSION = 55;

  @Test
  public void testProductionClassTargetsJava11() throws IOException {
    InputStream classBytes = Utils.class.getResourceAsStream("Utils.class");
    Assert.assertNotNull(classBytes, "The venice-common Utils.class resource must be available");
    try (DataInputStream classFile = new DataInputStream(classBytes)) {
      Assert.assertEquals(classFile.readInt(), 0xCAFEBABE, "Invalid Utils.class header");
      classFile.readUnsignedShort(); // Minor version
      Assert.assertEquals(classFile.readUnsignedShort(), JAVA_11_CLASS_FILE_MAJOR_VERSION, "Utils must target Java 11");
    }
  }
}
