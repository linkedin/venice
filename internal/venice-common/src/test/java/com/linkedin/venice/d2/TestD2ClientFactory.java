package com.linkedin.venice.d2;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertSame;

import com.linkedin.d2.balancer.D2Client;
import java.util.Optional;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestD2ClientFactory {
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    D2ClientFactory.setUnitTestMode();
  }

  @AfterClass(alwaysRun = true)
  public void teardown() {
    D2ClientFactory.resetUnitTestMode();
  }

  @Test
  public void getD2ClientFromFactory() {
    String d2ZkHost = "localhost:2181";
    D2Client d2Client = D2ClientFactory.getD2Client(d2ZkHost, Optional.empty());
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.size(), 1);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.getReferenceCount(d2ZkHost), 1);

    // Close D2Client should clean up and remove the client from the cache
    D2ClientFactory.release(d2ZkHost);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.size(), 0);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.getReferenceCount(d2ZkHost), 0);

    D2Client d2Client2 = D2ClientFactory.getD2Client(d2ZkHost, Optional.empty());
    D2Client d2Client3 = D2ClientFactory.getD2Client(d2ZkHost, Optional.empty());
    assertNotSame(d2Client, d2Client2);
    assertSame(d2Client2, d2Client3);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.size(), 1);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.getReferenceCount(d2ZkHost), 2);

    // Closing one D2Client should not clean up and remove the client from the cache
    D2ClientFactory.release(d2ZkHost);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.getReferenceCount(d2ZkHost), 1);

    // Closing the last shared D2Client should clean up and remove the client from the cache
    D2ClientFactory.release(d2ZkHost);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.size(), 0);
    assertEquals(D2ClientFactory.SHARED_OBJECT_FACTORY.getReferenceCount(d2ZkHost), 0);
  }
}
