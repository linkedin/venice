package com.linkedin.venice.router.httpclient;

import com.linkedin.venice.httpclient.CachedDnsResolver;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestCachedDnsResolver {
  private static class MockCachedDnsResolver extends CachedDnsResolver {
    private Map<String, List<InetAddress[]>> dnsMap = new HashMap<>();
    private Map<String, Integer> hostResolvedTimes = new HashMap<>();

    public static final String HOST1 = "host1.abc.com";
    public static final String HOST2 = "host2.abc.com";
    public static final String HOST3 = "host3.xyz.com";

    public MockCachedDnsResolver(String cachedHostPattern, long refreshIntervalInMs) throws UnknownHostException {
      super(cachedHostPattern, refreshIntervalInMs, null);
      // For host1.abc.com
      List<InetAddress[]> dnsEntriesForHost1 = new ArrayList<>();
      dnsEntriesForHost1.add(new InetAddress[] { InetAddress.getByName("127.0.0.1") });
      dnsEntriesForHost1.add(new InetAddress[] { InetAddress.getByName("127.0.0.2") });
      dnsEntriesForHost1.add(new InetAddress[] { InetAddress.getByName("127.0.0.3") });
      dnsMap.put(HOST1, dnsEntriesForHost1);
      hostResolvedTimes.put(HOST1, 0);
      // For host2.abc.com
      List<InetAddress[]> dnsEntriesForHost2 = new ArrayList<>();
      dnsEntriesForHost2.add(new InetAddress[] { InetAddress.getByName("127.0.1.1") });
      dnsEntriesForHost2.add(new InetAddress[] { InetAddress.getByName("127.0.1.2") });
      dnsEntriesForHost2.add(new InetAddress[] { InetAddress.getByName("127.0.1.3") });
      dnsMap.put(HOST2, dnsEntriesForHost2);
      hostResolvedTimes.put(HOST2, 0);
      // For host3.xyz.com
      List<InetAddress[]> dnsEntriesForHost3 = new ArrayList<>();
      dnsEntriesForHost3.add(new InetAddress[] { InetAddress.getByName("127.0.2.1") });
      dnsEntriesForHost3.add(new InetAddress[] { InetAddress.getByName("127.0.2.2") });
      dnsEntriesForHost3.add(new InetAddress[] { InetAddress.getByName("127.0.2.3") });
      dnsMap.put(HOST3, dnsEntriesForHost3);
      hostResolvedTimes.put(HOST3, 0);
    }

    @Override
    protected InetAddress[] systemGetAllByName(String host) throws UnknownHostException {
      if (!dnsMap.containsKey(host)) {
        throw new UnknownHostException(host);
      }
      List<InetAddress[]> socketAddressList = dnsMap.get(host);
      int times = hostResolvedTimes.get(host);
      hostResolvedTimes.put(host, times + 1);
      if (times >= socketAddressList.size()) {
        // always return last one
        return socketAddressList.get(socketAddressList.size() - 1);
      }
      return socketAddressList.get(times);
    }
  }

  private CachedDnsResolver cachedDnsResolver;

  @BeforeMethod
  public void setUp() throws UnknownHostException {
    cachedDnsResolver = new MockCachedDnsResolver(".*abc.com", 50);
  }

  @AfterMethod
  public void cleanUp() throws IOException {
    cachedDnsResolver.close();
  }

  @Test
  public void simpleTest() throws UnknownHostException {
    Assert.assertEquals(InetAddress.getByName("127.0.0.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST1)[0]);
    Assert.assertEquals(InetAddress.getByName("127.0.1.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST2)[0]);
    Assert.assertEquals(InetAddress.getByName("127.0.2.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST3)[0]);
    Assert.assertEquals(InetAddress.getByName("127.0.2.2"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST3)[0]);
  }

  @Test
  public void testCachingForMatchedHosts() throws UnknownHostException {
    Assert.assertEquals(InetAddress.getByName("127.0.0.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST1)[0]);
    Assert.assertEquals(InetAddress.getByName("127.0.0.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST1)[0]);

    Assert.assertEquals(InetAddress.getByName("127.0.1.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST2)[0]);
    Assert.assertEquals(InetAddress.getByName("127.0.1.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST2)[0]);

    Assert.assertEquals(InetAddress.getByName("127.0.2.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST3)[0]);
    Assert.assertEquals(InetAddress.getByName("127.0.2.2"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST3)[0]);
  }

  @Test
  public void testRefreshCacheEntries() throws UnknownHostException {
    Assert.assertEquals(InetAddress.getByName("127.0.0.1"), cachedDnsResolver.resolve(MockCachedDnsResolver.HOST1)[0]);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        Assert.assertNotEquals(
            InetAddress.getByName("127.0.0.1"),
            cachedDnsResolver.resolve(MockCachedDnsResolver.HOST1)[0]);
      } catch (UnknownHostException e) {
        Assert.fail(MockCachedDnsResolver.HOST1 + " should be a known host");
      }
    });

  }
}
