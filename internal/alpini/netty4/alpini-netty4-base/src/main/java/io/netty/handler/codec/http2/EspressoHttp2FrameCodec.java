package io.netty.handler.codec.http2;

import static io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT;

import com.linkedin.alpini.base.misc.ClassUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An extension of the {@link Http2FrameCodec} which provides a facility to
 * be able to block stream creation from clients without incurring any cost
 * of stream creation on the server side.
 */
public class EspressoHttp2FrameCodec extends Http2FrameCodec {
  private static final Logger LOG = LogManager.getLogger();

  private static final int BLOCKED_STREAMS_REMEMBER_LIMIT = 500;

  private static final Field DECORATING_HTTP2_CONNECTION_DECODER_DELEGATE =
      ClassUtil.getAccessibleField(DecoratingHttp2ConnectionDecoder.class, "delegate");

  private static final Field DEFAULT_HTTP2_CONNECTION_DECODER_FRAMEREADER =
      ClassUtil.getAccessibleField(DefaultHttp2ConnectionDecoder.class, "frameReader");

  EspressoHttp2FrameCodec(
      Http2ConnectionEncoder encoder,
      Http2ConnectionDecoder decoder,
      Http2Settings initialSettings,
      boolean decoupleCloseAndGoAway,
      Predicate<Channel> canCreateStreams) {
    super(
        encoder,
        new DelegateDecoder(decoder, Objects.requireNonNull(canCreateStreams), encoder.frameWriter()),
        initialSettings,
        decoupleCloseAndGoAway);
  }

  static class DelegateDecoder extends DecoratingHttp2ConnectionDecoder {
    private final Predicate<Channel> _canCreateStreams;
    private final Http2FrameWriter _frameWriter;
    private final Http2ConnectionDecoder _delegate;

    public DelegateDecoder(
        Http2ConnectionDecoder delegate,
        Predicate<Channel> canCreateStreams,
        Http2FrameWriter http2FrameWriter) {
      super(delegate);
      _canCreateStreams = canCreateStreams;
      _frameWriter = http2FrameWriter;
      _delegate = delegate; // store again because the superclass field is private
    }

    @Override
    public void frameListener(Http2FrameListener listener) {
      super.frameListener(new InterceptInstaller(listener));
    }

    @Override
    public Http2FrameListener frameListener() {
      Http2FrameListener frameListener = super.frameListener();
      // Unwrap the original Http2FrameListener as we add this decoder under the hood.
      if (frameListener instanceof InterceptInstaller) {
        return ((InterceptInstaller) frameListener)._delegate;
      }
      return frameListener;
    }

    class InterceptInstaller implements Http2FrameListener {
      private final Http2FrameListener _delegate;
      private boolean _installed;

      InterceptInstaller(Http2FrameListener delegate) {
        _delegate = delegate;
      }

      @Override
      public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
          throws Http2Exception {
        return _delegate.onDataRead(ctx, streamId, data, padding, endOfStream);
      }

      @Override
      public void onHeadersRead(
          ChannelHandlerContext ctx,
          int streamId,
          Http2Headers headers,
          int padding,
          boolean endOfStream) throws Http2Exception {
        _delegate.onHeadersRead(ctx, streamId, headers, padding, endOfStream);
      }

      @Override
      public void onHeadersRead(
          ChannelHandlerContext ctx,
          int streamId,
          Http2Headers headers,
          int streamDependency,
          short weight,
          boolean exclusive,
          int padding,
          boolean endOfStream) throws Http2Exception {
        _delegate.onHeadersRead(ctx, streamId, headers, streamDependency, weight, exclusive, padding, endOfStream);
      }

      @Override
      public void onPriorityRead(
          ChannelHandlerContext ctx,
          int streamId,
          int streamDependency,
          short weight,
          boolean exclusive) throws Http2Exception {
        _delegate.onPriorityRead(ctx, streamId, streamDependency, weight, exclusive);
      }

      @Override
      public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) throws Http2Exception {
        _delegate.onRstStreamRead(ctx, streamId, errorCode);
      }

      @Override
      public void onSettingsAckRead(ChannelHandlerContext ctx) throws Http2Exception {
        _delegate.onSettingsAckRead(ctx);
      }

      @Override
      public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) throws Http2Exception {
        _delegate.onSettingsRead(ctx, settings);
        if (!_installed) {
          try {
            Http2FrameReader instance = (Http2FrameReader) DEFAULT_HTTP2_CONNECTION_DECODER_FRAMEREADER
                .get(DECORATING_HTTP2_CONNECTION_DECODER_DELEGATE.get(DelegateDecoder.this._delegate));
            if (!(instance instanceof InterceptFrameReader)) {
              DEFAULT_HTTP2_CONNECTION_DECODER_FRAMEREADER.set(
                  DECORATING_HTTP2_CONNECTION_DECODER_DELEGATE.get(DelegateDecoder.this._delegate),
                  new InterceptFrameReader(instance));
              _installed = true;
            }
          } catch (Exception ex) {
            LOG.warn("Hijack escalation failed", ex); // TODO need less triggering log message
          }
        }

      }

      @Override
      public void onPingRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
        _delegate.onPingRead(ctx, data);
      }

      @Override
      public void onPingAckRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
        _delegate.onPingAckRead(ctx, data);
      }

      @Override
      public void onPushPromiseRead(
          ChannelHandlerContext ctx,
          int streamId,
          int promisedStreamId,
          Http2Headers headers,
          int padding) throws Http2Exception {
        _delegate.onPushPromiseRead(ctx, streamId, promisedStreamId, headers, padding);
      }

      @Override
      public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData)
          throws Http2Exception {
        _delegate.onGoAwayRead(ctx, lastStreamId, errorCode, debugData);
      }

      @Override
      public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement)
          throws Http2Exception {
        _delegate.onWindowUpdateRead(ctx, streamId, windowSizeIncrement);
      }

      @Override
      public void onUnknownFrame(
          ChannelHandlerContext ctx,
          byte frameType,
          int streamId,
          Http2Flags flags,
          ByteBuf payload) throws Http2Exception {
        _delegate.onUnknownFrame(ctx, frameType, streamId, flags, payload);
      }
    }

    class InterceptFrameReader implements Http2FrameReader {
      private final Http2FrameReader _http2FrameReader;

      InterceptFrameReader(Http2FrameReader http2FrameReader) {
        _http2FrameReader = http2FrameReader;
      }

      @Override
      public void readFrame(ChannelHandlerContext ctx, ByteBuf input, Http2FrameListener listener)
          throws Http2Exception {
        _http2FrameReader.readFrame(ctx, input, new InterceptFrameListener(listener));
      }

      @Override
      public Configuration configuration() {
        return _http2FrameReader.configuration();
      }

      @Override
      public void close() {
        _http2FrameReader.close();
      }
    }

    class InterceptFrameListener implements Http2FrameListener {
      private final Http2FrameListener _delegate;
      private final TreeSet<Integer> _blockedSet = new TreeSet<>();

      InterceptFrameListener(Http2FrameListener delegate) {
        _delegate = Objects.requireNonNull(delegate);
      }

      @Override
      public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
          throws Http2Exception {
        if (_blockedSet.contains(streamId)) {
          // we have to silently eat such packets because R2 will continue to send DATA frames even though
          // we sent a RST.
          // Client code will eventually see a ClosedChannelException within a R2 RemoteInvocationException
          return 0;
        }
        return _delegate.onDataRead(ctx, streamId, data, padding, endOfStream);
      }

      @Override
      public void onHeadersRead(
          ChannelHandlerContext ctx,
          int streamId,
          Http2Headers headers,
          int padding,
          boolean endOfStream) throws Http2Exception {
        onHeadersRead(ctx, streamId, headers, 0, DEFAULT_PRIORITY_WEIGHT, false, padding, endOfStream);
      }

      @Override
      public void onHeadersRead(
          ChannelHandlerContext ctx,
          int streamId,
          Http2Headers headers,
          int streamDependency,
          short weight,
          boolean exclusive,
          int padding,
          boolean endOfStream) throws Http2Exception {
        if (_blockedSet.contains(streamId)) {
          return;
        }

        if (connection().stream(streamId) == null) {
          if (headers.path() == null || !_canCreateStreams.test(ctx.channel())) {
            _frameWriter.writeRstStream(ctx, streamId, Http2Error.ENHANCE_YOUR_CALM.code(), ctx.voidPromise());
            _blockedSet.add(streamId);
            if (_blockedSet.size() > BLOCKED_STREAMS_REMEMBER_LIMIT) {
              _blockedSet.pollFirst();
            }
            return;
          }
        }

        _delegate.onHeadersRead(ctx, streamId, headers, streamDependency, weight, exclusive, padding, endOfStream);
      }

      @Override
      public void onPriorityRead(
          ChannelHandlerContext ctx,
          int streamId,
          int streamDependency,
          short weight,
          boolean exclusive) throws Http2Exception {
        if (_blockedSet.contains(streamId)) {
          return;
        }
        _delegate.onPriorityRead(ctx, streamId, streamDependency, weight, exclusive);
      }

      @Override
      public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) throws Http2Exception {
        if (_blockedSet.contains(streamId)) {
          _blockedSet.remove(streamId);
          return;
        }
        _delegate.onRstStreamRead(ctx, streamId, errorCode);
      }

      @Override
      public void onSettingsAckRead(ChannelHandlerContext ctx) throws Http2Exception {
        _delegate.onSettingsAckRead(ctx);
      }

      @Override
      public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) throws Http2Exception {
        _delegate.onSettingsRead(ctx, settings);
      }

      @Override
      public void onPingRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
        _delegate.onPingRead(ctx, data);
      }

      @Override
      public void onPingAckRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
        _delegate.onPingAckRead(ctx, data);
      }

      @Override
      public void onPushPromiseRead(
          ChannelHandlerContext ctx,
          int streamId,
          int promisedStreamId,
          Http2Headers headers,
          int padding) throws Http2Exception {
        _delegate.onPushPromiseRead(ctx, streamId, promisedStreamId, headers, padding);
      }

      @Override
      public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData)
          throws Http2Exception {
        _delegate.onGoAwayRead(ctx, lastStreamId, errorCode, debugData);
      }

      @Override
      public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement)
          throws Http2Exception {
        if (_blockedSet.contains(streamId)) {
          return;
        }
        _delegate.onWindowUpdateRead(ctx, streamId, windowSizeIncrement);
      }

      @Override
      public void onUnknownFrame(
          ChannelHandlerContext ctx,
          byte frameType,
          int streamId,
          Http2Flags flags,
          ByteBuf payload) throws Http2Exception {
        if (_blockedSet.contains(streamId)) {
          return;
        }
        _delegate.onUnknownFrame(ctx, frameType, streamId, flags, payload);
      }
    }
  }
}
