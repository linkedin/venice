package com.linkedin.alpini.netty4.pool;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestNettyDnsResolver {
  private static final Logger LOG = LogManager.getLogger(TestNettyDnsResolver.class);

  NioEventLoopGroup _nioEventLoopGroup;
  ChannelPoolResolver _resolver;

  @BeforeClass
  public void beforeClass() {
    _nioEventLoopGroup = new NioEventLoopGroup(4);
    _resolver = new NettyDnsResolver(NioDatagramChannel.class, _nioEventLoopGroup);
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    if (_nioEventLoopGroup != null) {
      _nioEventLoopGroup.shutdownGracefully();
    }
  }

  @Test
  public void basicLocalhostResolvedTest() throws Exception {

    InetSocketAddress resolved = new InetSocketAddress("localhost", 1245);
    Assert.assertFalse(resolved.isUnresolved());

    Future<InetSocketAddress> future = _resolver.resolve(resolved, ImmediateEventExecutor.INSTANCE.newPromise());

    InetSocketAddress address = future.get();

    Assert.assertSame(address, resolved);
  }

  private static String getLocalHostName() {
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      InetAddress address = InetAddress.getByAddress(localHost.getAddress());
      return address.getHostName();
    } catch (Exception ex) {
      return "localhost";
    }
  }

  @Test
  public void basicLocalhostTest() throws Exception {
    String localhost = getLocalHostName();
    LOG.error("localhost = {}", localhost);

    Future<InetSocketAddress> future = _resolver
        .resolve(InetSocketAddress.createUnresolved(localhost, 1234), ImmediateEventExecutor.INSTANCE.newPromise());

    InetSocketAddress address = future.get();

    Assert.assertEquals(address.getPort(), 1234);
    Assert.assertFalse(address.isUnresolved());

    ArrayDeque<Future<Future<InetSocketAddress>>> futures = new ArrayDeque<>(100);

    for (int i = 100; i > 0; i--) {

      futures.add(
          _nioEventLoopGroup.submit(
              () -> _resolver.resolve(
                  InetSocketAddress.createUnresolved(localhost, 1234),
                  ImmediateEventExecutor.INSTANCE.newPromise())));
    }

    for (Future<Future<InetSocketAddress>> f: futures) {
      InetSocketAddress addr = f.get().get();

      Assert.assertEquals(addr.getPort(), 1234);
      Assert.assertFalse(addr.isUnresolved());

    }
  }
}
