package com.linkedin.alpini.netty4.pool;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetSocketAddress;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 3/30/17.
 */
public class TestChannelPoolResolver {
  @Test(groups = "unit")
  public void testBasicDnsResolver() throws InterruptedException {
    BasicDnsResolver resolver = new BasicDnsResolver();

    Future<InetSocketAddress> result = resolver
        .resolve(InetSocketAddress.createUnresolved("localhost", 80), ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertFalse(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "localhost");
    Assert.assertEquals(result.getNow().getPort(), 80);
    Assert.assertEquals(result.getNow().getAddress().getHostAddress(), "127.0.0.1");

    result = resolver
        .resolve(InetSocketAddress.createUnresolved("google.com", 80), ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertFalse(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "google.com");
    Assert.assertEquals(result.getNow().getPort(), 80);

    result = resolver.resolve(
        InetSocketAddress.createUnresolved("unresolved.linkedin.com", 80),
        ImmediateEventExecutor.INSTANCE.newPromise());

    Assert.assertTrue(result.await().isSuccess());
    Assert.assertTrue(result.getNow().isUnresolved());
    Assert.assertEquals(result.getNow().getHostName(), "unresolved.linkedin.com");
    Assert.assertEquals(result.getNow().getPort(), 80);
  }
}
