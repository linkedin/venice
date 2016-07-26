package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import javax.validation.constraints.NotNull;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for Venice Utils
 */
public class UtilsTest {

  @Test
  public void testGetHelixNodeIdentifier() {
    int port = 1234;
    String identifier = Utils.getHelixNodeIdentifier(1234);
    Assert.assertEquals(identifier, Utils.getHostName() + "_" + port,
        "Identifier is not the valid format required by Helix.");
  }

  @Test
  public void testParseHostAndPortFromNodeIdentifier() {
    int port = 1234;
    String identifier = Utils.getHelixNodeIdentifier(1234);
    Assert.assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), Utils.getHostName());
    Assert.assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);

    identifier = "my_host_" + port;
    Assert.assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), "my_host");
    Assert.assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);

    identifier = "my_host_abc";
    Assert.assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), "my_host");
    try {
      Assert.assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);
      Assert.fail("Port should be numeric value");
    } catch (VeniceException e) {
      //expected
    }
  }
}
