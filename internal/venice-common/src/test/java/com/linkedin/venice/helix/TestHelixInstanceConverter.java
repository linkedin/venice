package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Instance;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for HelixInstanceConverter
 */
public class TestHelixInstanceConverter {
  @Test
  public void testConvertFromZNRecordToInstance() {
    String id = "testId";
    String host = "localhost";
    int httpPort = 1234;
    ZNRecord record = new ZNRecord(id);
    record.setSimpleField("HOST", host);
    record.setIntField("PORT", httpPort);

    Instance instance = HelixInstanceConverter.convertZNRecordToInstance(record);

    Assert.assertEquals(id, instance.getNodeId());
    Assert.assertEquals(host, instance.getHost());
    Assert.assertEquals(httpPort, instance.getPort());
  }

  @Test
  public void testConvertFromZNRecordToInstanceForInvalidPort() {
    String id = "testId";
    String host = "localhost";
    int httpPort = -100;
    int adminPort = 2345;
    ZNRecord record = new ZNRecord(id);
    record.setSimpleField("HOST", host);
    record.setIntField("HTTP_PORT", httpPort);
    record.setIntField("ADMIN_PORT", adminPort);
    try {
      HelixInstanceConverter.convertZNRecordToInstance(record);
      Assert.fail("Invalid port is assigned.");
    } catch (IllegalArgumentException iae) {
      // expected.
    }
  }

  @Test
  public void testConvertFromInstanceToZNRecord() {
    String id = "testId";
    String host = "localhost";
    int httpPort = 1234;
    Instance instance = new Instance(id, host, httpPort);
    ZNRecord record = HelixInstanceConverter.convertInstanceToZNRecord(instance);
    Assert.assertEquals(id, record.getId());
    Assert.assertEquals(host, record.getSimpleField("HOST"));
    Assert.assertEquals(httpPort, record.getIntField("PORT", -1));
  }
}
