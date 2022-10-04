package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;


/**
 * Created by acurtis on 4/26/18.
 */
@ChannelHandler.Sharable
public class HttpObjectToBasicHttpObjectAdapter extends ChannelInboundHandlerAdapter {
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest && !(msg instanceof BasicHttpRequest)) {
      if (msg instanceof FullHttpRequest) {
        msg = new BasicFullHttpRequest((FullHttpRequest) msg);
      } else {
        msg = new BasicHttpRequest((HttpRequest) msg);
      }
    } else if (msg instanceof HttpResponse && !(msg instanceof BasicHttpResponse)) {
      if (msg instanceof FullHttpResponse) {
        msg = new BasicFullHttpResponse((FullHttpResponse) msg);
      } else {
        msg = new BasicHttpResponse((HttpResponse) msg);
      }
    }
    super.channelRead(ctx, msg);
  }
}
