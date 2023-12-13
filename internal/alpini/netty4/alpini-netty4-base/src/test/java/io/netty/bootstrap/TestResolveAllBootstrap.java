package io.netty.bootstrap;

import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.util.TestNettyUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/23/18.
 */
public class TestResolveAllBootstrap {
  private EpollEventLoopGroup _group;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _group = TestNettyUtil.skipEpollIfNotFound(() -> new EpollEventLoopGroup());
  }

  @AfterClass(groups = "unit")
  public void afterClass() {
    Optional.ofNullable(_group).ifPresent(EpollEventLoopGroup::shutdownGracefully);
  }

  @Test(groups = "unit")
  public void testConnect0() throws Exception {
    CallTracker connectCallTracker = Mockito.mock(CallTracker.class);
    CallTracker resolveCallTracker = Mockito.mock(CallTracker.class);
    CallCompletion connectCallCompletion = Mockito.mock(CallCompletion.class, "connectCallCompletion");
    CallCompletion resolveCallCompletion = Mockito.mock(CallCompletion.class, "resolveCallCompletion");
    Mockito.when(connectCallTracker.startCall()).thenReturn(connectCallCompletion);
    Mockito.when(resolveCallTracker.startCall()).thenReturn(resolveCallCompletion);

    Bootstrap bootstrap =
        new ResolveAllBootstrap(connectCallTracker, resolveCallTracker).channel(EpollSocketChannel.class)
            .group(_group)
            .handler(new LoggingHandler());

    ServerSocket server = new ServerSocket(0);

    ChannelFuture connectFuture =
        bootstrap.clone().connect(InetSocketAddress.createUnresolved("localhost", server.getLocalPort()));

    Socket sock = server.accept();

    connectFuture.sync();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(resolveCallTracker).startCall();
    Mockito.verify(resolveCallCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(connectCallTracker).startCall();
    Mockito.verify(connectCallCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));

    server.close();
    sock.close();
    connectFuture.channel().close().sync();
  }

  @Test(groups = "unit")
  public void testConnectIpv6Ipv4() throws Exception {
    CallTracker connectCallTracker = Mockito.mock(CallTracker.class);
    CallTracker resolveCallTracker = Mockito.mock(CallTracker.class);
    CallCompletion connectCallCompletion = Mockito.mock(CallCompletion.class, "connectCallCompletion");
    CallCompletion resolveCallCompletion = Mockito.mock(CallCompletion.class, "resolveCallCompletion");
    Mockito.when(connectCallTracker.startCall()).thenReturn(connectCallCompletion);
    Mockito.when(resolveCallTracker.startCall()).thenReturn(resolveCallCompletion);

    // The sort will make it try the IPv6 loopback address first but since the server is bound
    // to the IPv4 loopback only, it will have to retry.

    Bootstrap bootstrap =
        new ResolveAllBootstrap(connectCallTracker, resolveCallTracker).sortResolvedAddress((o1, o2) -> {
          InetSocketAddress i1 = (InetSocketAddress) o1;
          InetSocketAddress i2 = (InetSocketAddress) o2;
          int v1 = i1.getAddress() instanceof Inet6Address ? -1 : +1;
          int v2 = i2.getAddress() instanceof Inet6Address ? -1 : +1;
          return Integer.compare(v1, v2);
        }).channel(EpollSocketChannel.class).group(_group).handler(new LoggingHandler());

    ServerSocket server = new ServerSocket();
    server.bind(new InetSocketAddress("127.0.0.1", 0));

    ChannelFuture connectFuture =
        bootstrap.remoteAddress(InetSocketAddress.createUnresolved("localhost", server.getLocalPort()))
            .clone()
            .connect();

    Socket sock = server.accept();

    connectFuture.sync();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(resolveCallTracker).startCall();
    Mockito.verify(resolveCallCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(connectCallTracker, Mockito.times(2)).startCall();
    Mockito.verify(connectCallCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));
    Mockito.verify(connectCallCompletion).closeCompletion(Mockito.isNull(), Mockito.isNotNull(Throwable.class));

    server.close();
    sock.close();
    connectFuture.channel().close().sync();
  }

  @Test(groups = "unit")
  public void testConnectRefused() throws Exception {
    CallTracker connectCallTracker = Mockito.mock(CallTracker.class);
    CallTracker resolveCallTracker = Mockito.mock(CallTracker.class);
    CallCompletion connectCallCompletion = Mockito.mock(CallCompletion.class, "connectCallCompletion");
    CallCompletion resolveCallCompletion = Mockito.mock(CallCompletion.class, "resolveCallCompletion");
    Mockito.when(connectCallTracker.startCall()).thenReturn(connectCallCompletion);
    Mockito.when(resolveCallTracker.startCall()).thenReturn(resolveCallCompletion);

    Bootstrap bootstrap =
        new ResolveAllBootstrap(connectCallTracker, resolveCallTracker).channel(EpollSocketChannel.class)
            .group(_group)
            .handler(new LoggingHandler());

    ChannelFuture connectFuture = bootstrap.clone()
        .remoteAddress(InetSocketAddress.createUnresolved("localhost", 79)) // doubt we have Gopher
        .connect();

    connectFuture.await();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(resolveCallTracker).startCall();
    Mockito.verify(resolveCallCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(connectCallTracker, Mockito.times(2)).startCall();
    Mockito.verify(connectCallCompletion, Mockito.times(2))
        .closeCompletion(Mockito.isNull(), Mockito.isNotNull(Throwable.class));

    Assert.assertTrue(connectFuture.isDone());
    Assert.assertFalse(connectFuture.isSuccess());
    Assert.assertTrue(
        connectFuture.cause().getMessage().matches(".*Connection refused: localhost/.*"),
        connectFuture.cause().getMessage());
  }

  @Test(groups = "unit")
  public void testConnectUnresolveable() throws Exception {
    CallTracker connectCallTracker = Mockito.mock(CallTracker.class);
    CallTracker resolveCallTracker = Mockito.mock(CallTracker.class);
    CallCompletion connectCallCompletion = Mockito.mock(CallCompletion.class, "connectCallCompletion");
    CallCompletion resolveCallCompletion = Mockito.mock(CallCompletion.class, "resolveCallCompletion");
    Mockito.when(connectCallTracker.startCall()).thenReturn(connectCallCompletion);
    Mockito.when(resolveCallTracker.startCall()).thenReturn(resolveCallCompletion);

    Bootstrap bootstrap =
        new ResolveAllBootstrap(connectCallTracker, resolveCallTracker).channel(EpollSocketChannel.class)
            .group(_group)
            .handler(new LoggingHandler());

    ChannelFuture connectFuture = bootstrap.clone()
        .remoteAddress(InetSocketAddress.createUnresolved("unresolvable.linkedin.com", 79)) // doubt we have Gopher
        .connect();

    connectFuture.await();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(resolveCallTracker).startCall();
    Mockito.verify(resolveCallCompletion).closeCompletion(Mockito.isNull(), Mockito.isNotNull(Throwable.class));

    Mockito.verifyNoMoreInteractions(connectCallTracker);
  }

  @Test(groups = "unit")
  public void testConnectResolvedRefused() throws Exception {
    CallTracker connectCallTracker = Mockito.mock(CallTracker.class);
    CallTracker resolveCallTracker = Mockito.mock(CallTracker.class);
    CallCompletion connectCallCompletion = Mockito.mock(CallCompletion.class, "connectCallCompletion");
    CallCompletion resolveCallCompletion = Mockito.mock(CallCompletion.class, "resolveCallCompletion");
    Mockito.when(connectCallTracker.startCall()).thenReturn(connectCallCompletion);
    Mockito.when(resolveCallTracker.startCall()).thenReturn(resolveCallCompletion);

    InetSocketAddress address = new InetSocketAddress("localhost", 79);
    Assert.assertFalse(address.isUnresolved());

    Bootstrap bootstrap =
        new ResolveAllBootstrap(connectCallTracker, resolveCallTracker).channel(EpollSocketChannel.class)
            .group(_group)
            .handler(new LoggingHandler());

    ChannelFuture connectFuture = bootstrap.clone()
        .remoteAddress(address) // doubt we have Gopher
        .connect();

    connectFuture.await();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verifyNoMoreInteractions(resolveCallTracker);
    Mockito.verifyNoMoreInteractions(resolveCallCompletion);

    // Since the address is already resolved, it will only make 1 connect attempt unlike
    // the earlier testConnectRefused test

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(connectCallTracker, Mockito.times(1)).startCall();
    Mockito.verify(connectCallCompletion, Mockito.times(1))
        .closeCompletion(Mockito.isNull(), Mockito.isNotNull(Throwable.class));

    Assert.assertTrue(connectFuture.isDone());
    Assert.assertFalse(connectFuture.isSuccess());
    Assert.assertTrue(
        connectFuture.cause().getMessage().matches(".*Connection refused: localhost/.*"),
        connectFuture.cause().getMessage());
  }
}
