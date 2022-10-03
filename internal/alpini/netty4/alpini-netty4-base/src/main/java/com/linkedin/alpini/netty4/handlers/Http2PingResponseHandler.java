package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2PingFrame;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/*
 * This handler is designed respond to Http2 ping frame. Before sending the ack,
 * each I/O worker is required to run a simple task (i.e, ()-> null) to check whether it is blocked.
 *
 * @author Binbing Hou <bhou@linkedin.com>
 * */
@ChannelHandler.Sharable
public class Http2PingResponseHandler extends SimpleChannelInboundHandler<Http2PingFrame> {
  private final static Logger LOG = LogManager.getLogger(Http2PingResponseHandler.class);

  private Http2PingFrame _lastPingAckFrame;

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2PingFrame http2PingFrame) throws Exception {

    Http2PingFrame pingAckFrame = new DefaultHttp2PingFrame(http2PingFrame.content(), true);
    _lastPingAckFrame = pingAckFrame;

    List<CompletableFuture<?>> futures = new ArrayList<>();
    addPingTasks(futures, ctx);

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenRun(() -> ctx.writeAndFlush(pingAckFrame).addListener(writeFuture -> {
          if (!writeFuture.isSuccess()) {
            LOG.warn(
                "Failed to send the PING ACK frame={} to Channel={}",
                pingAckFrame,
                ctx.channel(),
                writeFuture.cause());
          } else {
            LOG.debug("Succeeded to send the PING ACK frame={} to Channel={}", pingAckFrame, ctx.channel());
          }
        }));
  }

  public CompletableFuture<?> doSimpleTask(EventExecutor worker) {
    return CompletableFuture.supplyAsync(this::simpleTask, worker);
  }

  protected Object simpleTask() {
    LOG.debug("simple task");
    return null;
  }

  protected void addPingTasks(List<CompletableFuture<?>> taskList, ChannelHandlerContext ctx) {
    ctx.channel().eventLoop().parent().forEach(worker -> taskList.add(doSimpleTask(worker)));
  }

  /*package-private for test only*/ Http2PingFrame getLastPingAckFrame() {
    return _lastPingAckFrame;
  }
}
