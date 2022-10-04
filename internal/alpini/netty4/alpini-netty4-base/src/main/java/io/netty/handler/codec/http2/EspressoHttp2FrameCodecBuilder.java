package io.netty.handler.codec.http2;

import io.netty.channel.Channel;
import java.util.Objects;
import java.util.function.Predicate;


public class EspressoHttp2FrameCodecBuilder extends Http2FrameCodecBuilder {
  public static final Predicate<Channel> CAN_ALWAYS_CREATE_STREAMS = ch -> true;
  private Predicate<Channel> _canCreateStreams = Channel::isOpen;

  EspressoHttp2FrameCodecBuilder(boolean server) {
    super(server);
  }

  public EspressoHttp2FrameCodecBuilder canCreateStreams(Predicate<Channel> canCreateStreams) {
    _canCreateStreams = Objects.requireNonNull(canCreateStreams);
    return this;
  }

  /**
   * Creates a builder for a HTTP/2 server.
   */
  public static EspressoHttp2FrameCodecBuilder forServer() {
    return new EspressoHttp2FrameCodecBuilder(true);
  }

  /**
   * Creates a builder for an HTTP/2 client.
   * @return Builder for client
   */
  public static EspressoHttp2FrameCodecBuilder forClient() {
    return new EspressoHttp2FrameCodecBuilder(false);
  }

  @Override
  protected Http2FrameCodec build(
      Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder,
      Http2Settings initialSettings) {
    EspressoHttp2FrameCodec codec =
        new EspressoHttp2FrameCodec(encoder, decoder, initialSettings, decoupleCloseAndGoAway(), _canCreateStreams);
    codec.gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis());
    return codec;
  }
}
