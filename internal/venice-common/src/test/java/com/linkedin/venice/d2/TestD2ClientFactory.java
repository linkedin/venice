package com.linkedin.venice.d2;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.utils.ReferenceCounted;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;


public class TestD2ClientFactory {
  @Test
  public void getD2ClientFromFactory() throws NoSuchFieldException, IllegalAccessException {
    String d2ZkHost = "localhost:2181";
    ReferenceCounted<D2Client> d2Client = D2ClientFactory.getD2Client(d2ZkHost, Optional.empty(), true);
    Map<String, ReferenceCounted<D2Client>> d2ClientCache = D2ClientFactory.D2_CLIENT_CACHE;
    assertEquals(d2ClientCache.size(), 1);
    assertEquals(d2ClientCache.get(d2ZkHost).getReferenceCount(), 1);

    // Close D2Client should clean up and remove the client from the cache
    ReferenceCounted<D2Client> referenceCounted = d2ClientCache.get(d2ZkHost);
    referenceCounted.release();
    assertEquals(referenceCounted.getReferenceCount(), 0);
    assertFalse(d2ClientCache.containsKey(d2ZkHost));

    ReferenceCounted<D2Client> d2Client2 = D2ClientFactory.getD2Client(d2ZkHost, Optional.empty(), true);
    ReferenceCounted<D2Client> d2Client3 = D2ClientFactory.getD2Client(d2ZkHost, Optional.empty(), true);
    assertNotSame(d2Client, d2Client2);
    assertSame(d2Client2, d2Client3);
    assertEquals(d2ClientCache.size(), 1);
    assertEquals(d2ClientCache.get(d2ZkHost).getReferenceCount(), 2);

    // Closing one D2Client should not clean up and remove the client from the cache
    d2Client2.release();
    assertEquals(d2Client2.getReferenceCount(), 1);
    assertTrue(d2ClientCache.containsKey(d2ZkHost));

    // Closing the last shared D2Client should clean up and remove the client from the cache
    d2Client3.release();
    assertEquals(d2Client3.getReferenceCount(), 0);
    assertFalse(d2ClientCache.containsKey(d2ZkHost));
  }
}
