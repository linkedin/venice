package io.netty.handler.codec.http2;

/**
 * Utility class to expose package private members.
 */
public final class EspressoHttp2FrameCodecUtil {
  private EspressoHttp2FrameCodecUtil() {
  }

  /**
   * Calls {@code frameCodec.newStream()} and returns the result
   * @param frameCodec {@link Http2FrameCodec} instance
   * @return new {@link Http2FrameStream} instance
   */
  public static Http2FrameStream newStream(Http2FrameCodec frameCodec) {
    return frameCodec.newStream();
  }
}
