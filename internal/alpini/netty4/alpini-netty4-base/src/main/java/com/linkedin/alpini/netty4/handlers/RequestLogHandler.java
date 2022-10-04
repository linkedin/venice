package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.Msg;
import com.linkedin.alpini.base.misc.Time;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.AttributeKey;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * Created by acurtis on 4/14/17.
 */
@ChannelHandler.Sharable
public class RequestLogHandler extends ChannelInitializer<Channel> {
  private static final Logger LOG = LogManager.getLogger(RequestLogHandler.class);

  private static final AttributeKey<Dir> INBOUND_TYPE = AttributeKey.valueOf(RequestLogHandler.class, "inboundType");
  private static final AttributeKey<Dir> OUTBOUND_TYPE = AttributeKey.valueOf(RequestLogHandler.class, "outboundType");

  private enum Dir {
    Request, Response
  }

  /** Writes to a special logfile for the requests. */
  private final Logger _requestLog;

  /** Name of this pipeline (e.g. "inbound", "outbound", etc.). */
  private final String _pipelineName;

  private final String _pipelineKey;

  /**
   * Create a new RequestLogHandler which will write to the given named log. Direction specifies if this handler is used on
   * and inbound or outbound connection.
   * @param loggerName name of the logger. typically log4j.xml should send this logger to a separate file
   * @param pipelineName name of this pipeline; e.g. "inbound", "outbound", etc.
   */
  public RequestLogHandler(String loggerName, String pipelineName) {
    this(LogManager.getLogger(loggerName), pipelineName);
  }

  /* package private */ RequestLogHandler(Logger logger, String pipelineName) {
    _requestLog = logger;
    _pipelineName = pipelineName;
    _pipelineKey = "request-log-" + logger.getName() + "-" + pipelineName;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, _pipelineKey, new Handler());
  }

  class Handler extends ChannelDuplexHandler {
    ConnectInfo _connectInfo;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      catchException(LOG, () -> channelRead0(ctx, msg), "channelRead0");
      super.channelRead(ctx, msg);
    }

    private Void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      ConnectInfo connectInfo = getConnectInfo(ctx);
      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        ctx.channel().attr(INBOUND_TYPE).set(Dir.Request);
        handleHttpRequest(connectInfo, request);
      } else if (msg instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) msg;
        ctx.channel().attr(INBOUND_TYPE).set(Dir.Response);
        handleHttpResponse(connectInfo, response, true);
      }
      if (msg instanceof HttpContent) {
        HttpContent content = (HttpContent) msg;
        if (Dir.Request.equals(ctx.channel().attr(INBOUND_TYPE).get())) {
          if (content instanceof LastHttpContent) {
            handleLastHttpRequestContent(connectInfo, (LastHttpContent) content, true, true);
          } else {
            handleHttpRequestContent(connectInfo, content);
          }
        } else /* HttpResponse */ {
          if (content instanceof LastHttpContent) {
            handleLastHttpResponseContent(connectInfo, (LastHttpContent) content, true, true);
          } else {
            handleHttpRequestContent(connectInfo, content);
          }
        }
      }
      return null;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      catchException(LOG, () -> write0(ctx, msg, promise), "write0");
      super.write(ctx, msg, promise);
    }

    private Void write0(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      ConnectInfo connectInfo = getConnectInfo(ctx);
      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        ctx.channel().attr(OUTBOUND_TYPE).set(Dir.Request);
        handleHttpRequest(connectInfo, request);
      } else if (msg instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) msg;
        ctx.channel().attr(OUTBOUND_TYPE).set(Dir.Response);
        handleHttpResponse(connectInfo, response, false);
      }
      if (msg instanceof LastHttpContent) {
        LastHttpContent content = (LastHttpContent) msg;
        if (Dir.Request.equals(ctx.channel().attr(OUTBOUND_TYPE).get())) {
          promise.addListener(future -> {
            handleLastHttpRequestContent(connectInfo, content, future.isSuccess(), false);
          });
        } else /* HttpResponse */ {
          promise.addListener(future -> {
            handleLastHttpResponseContent(connectInfo, content, future.isSuccess(), false);
          });
        }
      }
      return null;
    }

    private ConnectInfo getConnectInfo(ChannelHandlerContext ctx) {
      return Optional.ofNullable(_connectInfo)
          .orElseGet(() -> _connectInfo = new ConnectInfo(ctx.channel().remoteAddress()));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      catchException(LOG, () -> channelActive0(ctx), "channelActive0");
      super.channelActive(ctx);
    }

    private Void channelActive0(ChannelHandlerContext ctx) throws Exception {
      // Log the CONNECT event and store ConnectInfo
      ConnectInfo connectInfo = getConnectInfo(ctx);

      if (!(ctx.channel() instanceof Http2StreamChannel)) {
        handleConnect(connectInfo);
      }
      return null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      catchException(LOG, () -> channelInactive0(ctx), "channelInactive0");
      super.channelInactive(ctx);
    }

    private Void channelInactive0(ChannelHandlerContext ctx) throws Exception {
      ConnectInfo connectInfo = getConnectInfo(ctx);

      if (!(ctx.channel() instanceof Http2StreamChannel)) {
        ctx.executor().submit(() -> handleDisconnect(connectInfo));
      }
      return null;
    }
  }

  void handleHttpRequest(ConnectInfo connectInfo, HttpRequest request) {
    // Create and store the new request info
    RequestInfo requestInfo = new RequestInfo(request);
    connectInfo._requestInfos.addLast(requestInfo);
  }

  void handleHttpRequestContent(ConnectInfo connectInfo, HttpContent content) {
    Optional.ofNullable(connectInfo._requestInfos.peekLast())
        .filter(requestInfo -> !requestInfo._hasContentLength)
        .ifPresent(
            requestInfo -> requestInfo._contentLength =
                Math.max(0, requestInfo._contentLength) + content.content().readableBytes());
  }

  void handleMissingRequestInfo(String method, ConnectInfo connectInfo, ResponseInfo responseInfo) {
    _requestLog.error(
        "{} {} without corresponding requestInfo or connectInfo. {}",
        _pipelineName,
        method,
        infoToString(_pipelineName, connectInfo, null, responseInfo));
  }

  void handleHttpResponse(ConnectInfo connectInfo, HttpResponse response, boolean inbound) {
    ResponseInfo responseInfo = new ResponseInfo(response, inbound);

    // Get the stored request and connection info. If either is null, that is an error
    // and we cannot log message information.
    RequestInfo requestInfo = connectInfo._requestInfos.peekFirst();

    if (requestInfo == null) {
      // Should never see a response without knowing about the connection or the request.
      handleMissingRequestInfo("handleHttpResponse", connectInfo, responseInfo);
    } else if (requestInfo._response != null) {
      _requestLog.error(
          "{} handleHttpResponse with duplicate responseInfo. {}",
          _pipelineName,
          infoToString(_pipelineName, connectInfo, requestInfo, responseInfo));
    } else {
      requestInfo._response = responseInfo;
    }
  }

  void handleLastHttpRequestContent(ConnectInfo connectInfo, LastHttpContent last, boolean success, boolean inbound) {
    // Get the stored request and connection info. If either is null, that is an error
    // and we cannot log message information.
    RequestInfo requestInfo = connectInfo._requestInfos.pollFirst();
    if (requestInfo == null) {
      // Should never see a response without knowing about the connection or the request.
      handleMissingRequestInfo("handleLastHttpRequestContent", connectInfo, null);
    } else {
      // Not done yet. This LastHttpContent is the end of the request portion.
      connectInfo._requestInfos.addFirst(requestInfo);
    }
  }

  void handleLastHttpResponseContent(ConnectInfo connectInfo, LastHttpContent last, boolean success, boolean inbound) {
    // Get the stored request and connection info. If either is null, that is an error
    // and we cannot log message information.
    RequestInfo requestInfo = connectInfo._requestInfos.pollFirst();
    if (requestInfo == null) {
      // Should never see a response without knowing about the connection or the request.
      handleMissingRequestInfo("handleLastHttpResponseContent", connectInfo, null);
    } else if (requestInfo._response != null) {
      ResponseInfo responseInfo = requestInfo._response;
      responseInfo._completeMillis = Time.currentTimeMillis();
      if (last != null && last.trailingHeaders().contains(HttpHeaderNames.CONTENT_LENGTH)) {
        try {
          responseInfo._contentLength =
              Long.parseUnsignedLong(last.trailingHeaders().get(HttpHeaderNames.CONTENT_LENGTH));
        } catch (Exception ignored) {
        }
      }
      if (success && requestInfo._response._status.codeClass() != HttpStatusClass.SERVER_ERROR) {
        _requestLog.debug("{}", infoToString(_pipelineName, connectInfo, requestInfo, responseInfo));
      } else {
        _requestLog.error("{}", infoToString(_pipelineName, connectInfo, requestInfo, responseInfo));
      }
    } else {
      _requestLog.error("{}", infoToString(_pipelineName, connectInfo, requestInfo, null));
    }
  }

  void handleConnect(ConnectInfo connectInfo) {
    _requestLog.info("{} {} CONNECTED", _pipelineName, connectInfo._remoteAddress);
  }

  void handleDisconnect(ConnectInfo connectInfo) {
    if (connectInfo == null) {
      _requestLog.error("{} handleHttpResponse without corresponding connectInfo.", _pipelineName);
    } else {
      long now = Time.currentTimeMillis();

      // If there was a request that never received a response before the channel was closed (timeout?), log it.
      while (!connectInfo._requestInfos.isEmpty()) {
        RequestInfo requestInfo = connectInfo._requestInfos.removeFirst();

        long durationMillis = now - requestInfo._startMillis;
        _requestLog.info(
            "{} {} {} {} {} --> CHANNEL-CLOSED {}",
            _pipelineName,
            connectInfo._remoteAddress,
            requestInfo._protocolVersion,
            requestInfo._method,
            requestInfo._uri,
            durationMillis);
      }

      _requestLog
          .info("{} {} DISCONNECTED {}", _pipelineName, connectInfo._remoteAddress, now - connectInfo._startMillis);
    }
  }

  <T> void catchException(Logger log, Callable<T> callable, String methodName) {
    try {
      callable.call();
    } catch (Throwable ex) {
      log.warn("Error in {}", methodName, ex);
    }
  }

  /* package private */ static Msg infoToString(
      String pipelineName,
      ConnectInfo connectInfo,
      RequestInfo requestInfo,
      ResponseInfo responseInfo) {
    return Msg.make(() -> (StringBuilderFormattable) sb -> {
      sb.append(pipelineName).append(' ');
      if (connectInfo != null) {
        sb.append(connectInfo._remoteAddress).append(' ');
      } else {
        sb.append("UNKNOWN ");
      }
      if (requestInfo == null) {
        sb.append("UNKNOWN UNKNOWN UNKNOWN UNKNOWN");
      } else {
        sb.append(requestInfo._protocolVersion)
            .append(' ')
            .append(requestInfo._method)
            .append(' ')
            .append(requestInfo._uri)
            .append(' ');
        if (requestInfo._contentLengthException) {
          sb.append("UNKNOWN");
        } else {
          sb.append(requestInfo._contentLength);
        }
      }
      sb.append("--> ");
      if (responseInfo == null) {
        sb.append("UNKNOWN UNKNOWN UNKNOWN UNKNOWN UNKNOWN");
      } else {
        sb.append(responseInfo._status).append(' ');
        if (responseInfo._contentLengthException) {
          sb.append("UNKNOWN");
        } else {
          sb.append(responseInfo._contentLength);
        }
        sb.append(' ').append(responseInfo._contentLocation).append(' ');
        if (requestInfo != null) {
          sb.append(responseInfo._endMillis - requestInfo._startMillis);
        } else {
          sb.append("UNKNOWN");
        }
        sb.append(' ').append(responseInfo._completeMillis - responseInfo._endMillis);
      }
    });
  }

  /* package private */ static final class ConnectInfo {
    ConnectInfo(SocketAddress remoteAddress) {
      _startMillis = Time.currentTimeMillis();
      _remoteAddress = remoteAddress;
    }

    SocketAddress _remoteAddress;
    long _startMillis;

    final LinkedList<RequestInfo> _requestInfos = new LinkedList<>();
  }

  /* package private */ static final class RequestInfo {
    RequestInfo(HttpRequest request) {
      _startMillis =
          request instanceof BasicRequest ? ((BasicRequest) request).getRequestTimestamp() : Time.currentTimeMillis();
      _uri = request.uri();
      _method = request.method();
      _protocolVersion = request.protocolVersion();
      try {
        _contentLength = HttpUtil.getContentLength(request, -1);
        _hasContentLength = _contentLength >= 0;
      } catch (Exception ignored) {
        _contentLengthException = true;
      }
    }

    String _uri;
    HttpMethod _method;
    HttpVersion _protocolVersion;
    long _startMillis;
    long _contentLength;
    boolean _contentLengthException;
    boolean _hasContentLength;

    ResponseInfo _response;
  }

  /* package private */ static final class ResponseInfo {
    ResponseInfo(HttpResponse response, boolean inbound) {
      _endMillis = Time.currentTimeMillis();
      try {
        _contentLength = HttpUtil.getContentLength(response, -1);
        _hasContentLength = _contentLength >= 0;
      } catch (Exception ignored) {
        _contentLengthException = true;
      }
      _contentLocation = response.headers().get(HttpHeaderNames.CONTENT_LOCATION);
      _status = response.status();
      _inbound = inbound;
    }

    long _endMillis;
    long _completeMillis;
    long _contentLength;
    boolean _contentLengthException;
    String _contentLocation;
    HttpResponseStatus _status;
    boolean _inbound;
    boolean _hasContentLength;
  }
}
