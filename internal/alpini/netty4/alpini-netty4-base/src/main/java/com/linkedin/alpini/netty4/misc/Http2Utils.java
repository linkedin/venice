package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.netty4.pool.Http2AwareChannelPool;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.EspressoHttp2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ChannelDuplexHandler;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * HTTP/2 utility methods.
 *
 * @author Yaoming Zhan <yzhan@linkedin.com>
 */
public final class Http2Utils {
  public static final AttributeKey<CompletableFuture<Http2FrameStream>> FRAME_STREAM_KEY =
      AttributeKey.valueOf("frameStream");
  // Netty doesn't have this exposed as a constant
  public static final String MAXIMUM_ACTIVE_STREAMS_VIOLATED_MSG = "Maximum active streams violated for this endpoint.";
  private static final String MAX_ACTIVE_STREAMS_VIOLATED_MSG_PREFIX = "Maximum active streams";

  private Http2Utils() {
    throw new UnsupportedOperationException();
  }

  public static Http2Connection http2Connection(Channel parentChannel) {
    return parentChannel.attr(Http2AwareChannelPool.HTTP2_CONNECTION).get();
  }

  public static Http2Connection.Endpoint<Http2LocalFlowController> localEndpoint(Channel parentChannel) {
    return http2Connection(parentChannel).local();
  }

  /**
   * Checks if the local HTTP/2 endpoint can open a new stream.
   * @param parentChannel Parent HTTP/2 channel
   * @return true if it can open new stream
   */
  public static boolean canOpenLocalStream(Channel parentChannel) {
    return localEndpoint(parentChannel).canOpenStream();
  }

  /**
   * Checks if an exception is thrown because of too many active streams.
   * @param t Exception
   * @return true if caused by active streams limit
   */
  public static boolean isTooManyActiveStreamsError(Throwable t) {
    if (t instanceof Http2Exception) {
      Http2Exception ex = (Http2Exception) t;
      // PROTOCOL_ERROR is used as active stream limit reached error code in DefaultHttp2Connection.DefaultStream#open
      return (ex.error() == Http2Error.REFUSED_STREAM || ex.error() == Http2Error.PROTOCOL_ERROR)
          // Some other messages will be used by Netty for other types of REFUSED_STREAM scenario, so check the message
          && String.valueOf(ex.getMessage()).startsWith(MAX_ACTIVE_STREAMS_VIOLATED_MSG_PREFIX);
    }
    return false;
  }

  public static void closeStreamChannel(Http2StreamChannel streamChannel) {
    try {
      if (!streamChannel.closeFuture().isDone()) {
        streamChannel.close();
      }
    } catch (Exception ex) {
      // eat this
    }
  }

  public static Http2ChannelDuplexHandler http2MultiplexHandler(boolean channelReuse, ChannelPipeline pipeline) {
    return channelReuse ? pipeline.get(EspressoHttp2MultiplexHandler.class) : pipeline.get(Http2MultiplexHandler.class);
  }

  public static boolean isHttp2MultiplexPipeline(boolean channelReuse, ChannelPipeline pipeline) {
    return http2MultiplexHandler(channelReuse, pipeline) != null;
  }

  @Deprecated
  public static boolean isHttp2Pipeline(ChannelPipeline pipeline) {
    return isHttp2ParentChannelPipeline(pipeline);
  }

  public static boolean isHttp2ParentChannelPipeline(ChannelPipeline pipeline) {
    return pipeline.context(Http2ConnectionHandler.class) != null;
  }

  /**
   * Checks if a stream channel is currently available for reuse.
   * @param streamChannel Stream channel
   * @return true if available for reuse
   */
  public static boolean channelAvailableForReuse(Http2StreamChannel streamChannel) {
    return NettyUtils.isTrue(streamChannel, Http2AwareChannelPool.HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE);
  }

  /**
   * Checks if a stream channel is a previously reused channel.
   * @param streamChannel Stream channel
   * @return true if reused
   */
  public static boolean isReusedChannel(Http2StreamChannel streamChannel) {
    return NettyUtils.isTrue(streamChannel, Http2AwareChannelPool.HTTP2_REUSED_STREAM_CHANNEL);
  }

  /**
   * Mark a stream to indicate that it will be reused.
   * @param streamChannel Stream channel
   */
  public static void markChannelForReuse(Http2StreamChannel streamChannel) {
    streamChannel.attr(Http2AwareChannelPool.HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.FALSE);
    streamChannel.attr(Http2AwareChannelPool.HTTP2_REUSED_STREAM_CHANNEL).set(Boolean.TRUE);
  }

  /**
   * Unmark a stream channel to indicate that reuse is completed.
   * @param streamChannel Stream channel
   */
  public static void unmarkReusedStream(Http2StreamChannel streamChannel) {
    streamChannel.attr(Http2AwareChannelPool.HTTP2_REUSED_STREAM_CHANNEL).set(null);
  }

  public static void markChannelAvailableForReuse(Http2StreamChannel streamChannel) {
    streamChannel.attr(Http2AwareChannelPool.HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
  }

  /**
   * Creates an {@link Http2Exception} for having too many concurrent requests.
   * @param msg Exception message
   * @return Exception
   */
  public static Http2Exception tooManyStreamsException(String msg) {
    return new Http2Exception(Http2Error.REFUSED_STREAM, MAXIMUM_ACTIVE_STREAMS_VIOLATED_MSG + " " + msg);
  }

  public static boolean isUnexpectedError(long errorCode, boolean fromRemote) {
    return isUnexpectedError(Http2Error.valueOf(errorCode), fromRemote);
  }

  public static boolean isUnexpectedError(@Nullable Http2Error errorEnum) {
    return isUnexpectedError(errorEnum, false);
  }

  /**
   * Returns true, if the enum is a known error or unknown error code.
   */
  public static boolean isUnexpectedError(@Nullable Http2Error errorEnum, boolean fromRemote) {
    if (null != errorEnum) {
      switch (errorEnum) {
        case NO_ERROR:
          return false;
        case CANCEL:
          // When we receive RST from remote with CANCEL, we treat is as an error
          return fromRemote;
        case PROTOCOL_ERROR:
        case INTERNAL_ERROR:
        case FLOW_CONTROL_ERROR:
        case SETTINGS_TIMEOUT:
        case STREAM_CLOSED:
        case FRAME_SIZE_ERROR:
        case REFUSED_STREAM:
        case COMPRESSION_ERROR:
        case CONNECT_ERROR:
        case ENHANCE_YOUR_CALM:
        case INADEQUATE_SECURITY:
        case HTTP_1_1_REQUIRED:
        default:
          return true;
      }
    }
    return true;
  }

  /**
   * Check whether the channel is a http2 connection that has been configured.
   * @param parentChannel http Channel
   */
  public static boolean isConfiguredHttp2Connection(@Nonnull Channel parentChannel) {
    return parentChannel.hasAttr(Http2AwareChannelPool.HTTP2_CONNECTION)
        && null != parentChannel.attr(Http2AwareChannelPool.HTTP2_CONNECTION).get();
  }

  public static Future<Http2FrameStream> frameStream(AttributeMap map) {
    return map.hasAttr(FRAME_STREAM_KEY) ? map.attr(FRAME_STREAM_KEY).get() : null;
  }

  public static void setFrameStream(AttributeMap map, Http2FrameStream frameStream) {
    if (map.hasAttr(FRAME_STREAM_KEY) && map.attr(FRAME_STREAM_KEY).get() != null
        && !map.attr(FRAME_STREAM_KEY).get().isDone()) {
      map.attr(FRAME_STREAM_KEY).get().complete(frameStream);
    }
  }
}
