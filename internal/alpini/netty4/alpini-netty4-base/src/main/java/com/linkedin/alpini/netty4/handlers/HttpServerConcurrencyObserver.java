package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 12/6/17.
 */
public class HttpServerConcurrencyObserver extends ChannelDuplexHandler {
  private final CallTracker _callTracker;
  private final Queue<CallCompletion> _callCompletionQueue = new LinkedList<>();
  private HttpResponseStatus _httpResponseStatus;

  public HttpServerConcurrencyObserver(@Nonnull CallTracker callTracker) {
    assert !isSharable();
    _callTracker = Objects.requireNonNull(callTracker);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpResponse) {
      if (((HttpResponse) msg).status().codeClass() != HttpStatusClass.INFORMATIONAL) {
        _httpResponseStatus = ((HttpResponse) msg).status();
      }
    }
    if (msg instanceof LastHttpContent) {
      HttpResponseStatus status = _httpResponseStatus;
      CallCompletion callCompletion = _callCompletionQueue.remove();
      promise.addListener(future -> {
        if (future.isSuccess() && status != null && status.codeClass() != HttpStatusClass.SERVER_ERROR) {
          callCompletion.close();
        } else {
          callCompletion.closeWithError();
        }
      });
      _httpResponseStatus = null;
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    _callCompletionQueue.forEach(CallCompletion::close);
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest) {
      _callCompletionQueue.add(_callTracker.startCall());
    }
    super.channelRead(ctx, msg);
  }
}
