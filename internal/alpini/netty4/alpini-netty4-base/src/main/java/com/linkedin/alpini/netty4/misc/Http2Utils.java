package com.linkedin.alpini.netty4.misc;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import javax.annotation.Nullable;


/**
 * HTTP/2 utility methods.
 *
 * @author Yaoming Zhan <yzhan@linkedin.com>
 */
public final class Http2Utils {
  private static final String MAX_ACTIVE_STREAMS_VIOLATED_MSG_PREFIX = "Maximum active streams";

  private Http2Utils() {
    throw new UnsupportedOperationException();
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

  @Deprecated
  public static boolean isHttp2Pipeline(ChannelPipeline pipeline) {
    return isHttp2ParentChannelPipeline(pipeline);
  }

  public static boolean isHttp2ParentChannelPipeline(ChannelPipeline pipeline) {
    return pipeline.context(Http2ConnectionHandler.class) != null;
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
}
