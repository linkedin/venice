package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import org.mockito.Mockito;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestHeaderStamp {
  @Test(groups = "unit")
  public void testStampHeader() throws Exception {
    HeaderStamp stamp = new HeaderStamp("TestHeader", "TestValue");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);

    HttpResponse msg = Mockito.mock(HttpResponse.class);
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    Mockito.when(msg.headers()).thenReturn(headers);

    stamp.write(ctx, msg, promise);

    Mockito.verify(msg).headers();
    Mockito.verify(headers).add(Mockito.eq("TestHeader"), Mockito.eq("TestValue"));
    Mockito.verify(ctx).write(Mockito.eq(msg), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx, msg, promise, headers);

  }

  @Test(groups = "unit")
  public void testNoStampHeader() throws Exception {

    HeaderStamp stamp = new HeaderStamp("TestHeader", "TestValue");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Object msg = Mockito.mock(Object.class);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    stamp.write(ctx, msg, promise);

    Mockito.verify(ctx).write(Mockito.eq(msg), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx, msg, promise);
  }
}
