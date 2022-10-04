package io.netty.bootstrap;

import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/23/18.
 */
public class TestInstrumentedBootstrap {
  private NioEventLoopGroup _group;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _group = new NioEventLoopGroup();
  }

  @AfterClass(groups = "unit")
  public void afterClass() {
    Optional.ofNullable(_group).ifPresent(NioEventLoopGroup::shutdownGracefully);
  }

  @Test(groups = "unit")
  public void testConnect1() throws Exception {

    CallTracker callTracker = Mockito.mock(CallTracker.class);
    CallCompletion callCompletion = Mockito.mock(CallCompletion.class, "testConnect1");
    Mockito.when(callTracker.startCall()).thenReturn(callCompletion);

    Bootstrap bootstrap = new InstrumentedBootstrap(callTracker).channel(NioSocketChannel.class)
        .group(_group)
        .handler(new LoggingHandler());

    ServerSocket server = new ServerSocket(0);

    ChannelFuture connectFuture = bootstrap.clone().connect(server.getLocalSocketAddress());

    Socket sock = server.accept();

    connectFuture.sync();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(callTracker).startCall();
    Mockito.verify(callCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));

    server.close();
    sock.close();
    connectFuture.channel().close().sync();
  }

  @Test(groups = "unit")
  public void testConnect0() throws Exception {
    CallTracker callTracker = Mockito.mock(CallTracker.class);
    CallCompletion callCompletion = Mockito.mock(CallCompletion.class, "testConnect0");
    Mockito.when(callTracker.startCall()).thenReturn(callCompletion);

    Bootstrap bootstrap = new InstrumentedBootstrap(callTracker).channel(NioSocketChannel.class)
        .group(_group)
        .handler(new LoggingHandler());

    ServerSocket server = new ServerSocket(0);

    ChannelFuture connectFuture = bootstrap.clone().remoteAddress(server.getLocalSocketAddress()).connect();

    Socket sock = server.accept();

    connectFuture.sync();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(callTracker).startCall();
    Mockito.verify(callCompletion).closeCompletion(Mockito.any(), Mockito.isNull(Throwable.class));

    server.close();
    sock.close();
    connectFuture.channel().close().sync();
  }

  @Test(groups = "unit")
  public void testConnectError() throws Exception {
    CallTracker callTracker = Mockito.mock(CallTracker.class);
    CallCompletion callCompletion = Mockito.mock(CallCompletion.class, "testConnect0");
    Mockito.when(callTracker.startCall()).thenReturn(callCompletion);

    Bootstrap bootstrap = new InstrumentedBootstrap(callTracker).channel(NioSocketChannel.class)
        .group(_group)
        .handler(new LoggingHandler());

    ServerSocket server = new ServerSocket(0);
    bootstrap.remoteAddress(server.getLocalSocketAddress());
    server.close();

    ChannelFuture connectFuture = bootstrap.clone().connect();

    connectFuture.await();
    Thread.sleep(100);

    // noinspection ResultOfMethodCallIgnored
    Mockito.verify(callTracker).startCall();
    Mockito.verify(callCompletion).closeCompletion(Mockito.isNull(), Mockito.isNotNull(ConnectException.class));
  }

}
