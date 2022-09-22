package com.linkedin.alpini.router.impl.netty4;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestChannelOption {
  @Test(groups = "unit")
  public void testChannelOptions() {
    Assert.assertTrue(ChannelOption.exists(EpollChannelOption.TCP_DEFER_ACCEPT.name() + ""));
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*TCP_DEFER_ACCEPT has an incompatible type value: Long")
  public void testChannelOptionBadArg() {
    ServerBootstrap boot = new ServerBootstrap();
    Router4Impl.setChannelOption(boot, EpollChannelOption.TCP_DEFER_ACCEPT.name(), 1000L);
  }

  @Test(groups = "unit")
  public void testChannelOptionGoodArg() {
    ServerBootstrap boot = new ServerBootstrap();
    Router4Impl.setChannelOption(boot, EpollChannelOption.TCP_DEFER_ACCEPT.name(), 1000);
    Assert.assertEquals(boot.config().options().get(EpollChannelOption.TCP_DEFER_ACCEPT), 1000);
  }

  @Test(groups = "unit", expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "value")
  public void testChannelOptionNull() {
    ServerBootstrap boot = new ServerBootstrap();
    Router4Impl.setChannelOption(boot, ChannelOption.ALLOCATOR.name(), null);
  }

  @Test(groups = "unit")
  public void testChannelOptionChild() {
    ServerBootstrap boot = new ServerBootstrap();
    Router4Impl.setChannelOption(boot, "CHILD." + ChannelOption.ALLOCATOR.name(), UnpooledByteBufAllocator.DEFAULT);
    Assert.assertSame(boot.config().childOptions().get(ChannelOption.ALLOCATOR), UnpooledByteBufAllocator.DEFAULT);
    Router4Impl.setChannelOption(boot, "CHILD." + ChannelOption.ALLOCATOR.name(), PooledByteBufAllocator.DEFAULT);
    Assert.assertSame(boot.config().childOptions().get(ChannelOption.ALLOCATOR), PooledByteBufAllocator.DEFAULT);
  }

  @Test(groups = "unit")
  public void testChannelOptionServer() {
    ServerBootstrap boot = new ServerBootstrap();
    Router4Impl.setChannelOption(boot, ChannelOption.ALLOCATOR.name(), UnpooledByteBufAllocator.DEFAULT);
    Assert.assertSame(boot.config().options().get(ChannelOption.ALLOCATOR), UnpooledByteBufAllocator.DEFAULT);
    Router4Impl.setChannelOption(boot, ChannelOption.ALLOCATOR.name(), PooledByteBufAllocator.DEFAULT);
    Assert.assertSame(boot.config().options().get(ChannelOption.ALLOCATOR), PooledByteBufAllocator.DEFAULT);
  }

  @Test(groups = "unit")
  public void testChannelOptionUnknown() {
    ServerBootstrap boot = new ServerBootstrap();
    Router4Impl.setChannelOption(boot, "CHILD.FOO", UnpooledByteBufAllocator.DEFAULT);
    Assert.assertTrue(boot.config().options().isEmpty());
    Assert.assertTrue(boot.config().childOptions().isEmpty());
  }
}
