package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicHttpMultiPart;
import com.linkedin.alpini.netty4.misc.BasicHttpObjectDecoder;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpConstants;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;
import io.netty.util.ByteProcessor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Decodes {@linkplain ByteBuf} objects as {@link com.linkedin.alpini.netty4.misc.HttpMultiPart} headers and
 * content.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class HttpContentMultiPartDecode extends BasicHttpObjectDecoder {
  private static final Logger LOG = LogManager.getLogger(HttpContentMultiPartDecode.class);
  private static final ByteBuf DUMMY_HEADER = Unpooled.copiedBuffer("1 2 3\r\n", StandardCharsets.US_ASCII);
  private static final ByteProcessor FIND_NON_CONTROL_CHARS = value -> (0xff & (int) value) < 32;

  private static final byte CR = HttpConstants.CR;
  private static final byte LF = HttpConstants.LF;

  private final HttpContentUnwrap _unwrap;
  private final int _maxChunkSize;
  private int _lastReadable;
  private boolean _inPart;
  private int _innerDecodeLast;
  private int _decodeLast;

  public HttpContentMultiPartDecode(
      @Nonnull HttpContentUnwrap unwrap,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders,
      int initialBufferSize) {
    super(
        10,
        checkSize(maxHeaderSize, "maxHeaderSize"),
        checkSize(maxChunkSize, "maxChunkSize"),
        false,
        validateHeaders,
        initialBufferSize);
    _unwrap = unwrap;
    _maxChunkSize = maxChunkSize;
  }

  private static int checkSize(int size, String message) {
    if (size < 512) {
      throw new IllegalArgumentException("expect " + message + " >= 512");
    }
    return size;
  }

  private void sanityCheckBeforeDecode(ByteBuf in, int size) {
    String message;
    State currentState = getDecoderState();
    if (State.SKIP_CONTROL_CHARS == currentState) {
      if (in.forEachByte(in.readerIndex(), size, FIND_NON_CONTROL_CHARS) < 0) {
        return;
      }
      message = "Missing content boundary, expect control chars: ";
    } else if (State.READ_INITIAL == currentState) {
      message = "Missing content boundary: ";
    } else {
      return;
    }
    throw new IllegalStateException(message + ByteBufUtil.prettyHexDump(in.slice(in.readerIndex(), size)));
  }

  private void decodeSlice(ChannelHandlerContext ctx, ByteBuf in, int size, List<Object> out) throws Exception {
    in.markReaderIndex();

    // Sanity check the state of the decoder
    sanityCheckBeforeDecode(in, size);

    ByteBuf slice = NettyUtils.read(in, size);
    try {
      super.decode(ctx, slice, out);
    } finally {
      if (slice.isReadable()) {
        in.resetReaderIndex().skipBytes(size - slice.readableBytes());
      }
      slice.release();
    }
  }

  private void passThroughChunks(ChannelHandlerContext ctx, ByteBuf in, int endPos, List<Object> out) throws Exception {
    while (endPos - in.readerIndex() > _maxChunkSize) {
      if (_inPart) {
        decodeSlice(ctx, in, _maxChunkSize, out);
      } else {
        in.skipBytes(_maxChunkSize);
      }
    }
  }

  private boolean passThroughRemaining(ChannelHandlerContext ctx, ByteBuf in, int boundaryPos, List<Object> out)
      throws Exception {
    if (_inPart) {
      while (State.READ_HEADER == getDecoderState() && in.readerIndex() < boundaryPos) {
        decodeSlice(ctx, in, boundaryPos - in.readerIndex(), out);
      }

      if (State.SKIP_CONTROL_CHARS != getDecoderState()) {
        while (in.readerIndex() < boundaryPos) {
          decodeSlice(ctx, in, boundaryPos - in.readerIndex(), out);
        }
        if (State.READ_FIXED_LENGTH_CONTENT == getDecoderState()) {
          return true;
        }
        last(ctx, Unpooled.EMPTY_BUFFER, out);
      }
    } else {
      in.readerIndex(boundaryPos);
    }
    return false;
  }

  private static boolean isCRLF(ByteBuf in, int index) {
    return in.getByte(index) == CR && in.getByte(index + 1) == LF;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (_innerDecodeLast > 0) {
      super.decode(ctx, in, out);
      _lastReadable = 0;
    } else if (_unwrap._contentType != null && _unwrap._contentType.isMultipart()) {
      AsciiString boundary = _unwrap._boundary;
      final int startIndex = in.readerIndex();
      int pos = startIndex + Math.max(0, _lastReadable - boundary.length() - 4);
      ByteBuf boundaryByteBuf = Unpooled.wrappedBuffer(boundary.array(), boundary.arrayOffset(), boundary.length());

      while (pos < in.writerIndex()) {

        // If the multipart contained a content length, we should just consume it all without
        // looking for part boundaries.
        if (State.READ_FIXED_LENGTH_CONTENT == getDecoderState()) {
          super.decode(ctx, in, out);
          pos = in.readerIndex();
          continue;
        }

        int boundaryPos = ByteBufUtil.indexOf(in, pos, in.writerIndex(), (byte) '-');

        if (boundaryPos < 0) {
          passThroughChunks(ctx, in, in.writerIndex() - 2, out);
          break;
        }

        passThroughChunks(ctx, in, boundaryPos - 2, out);

        if (boundaryPos + boundary.length() + 2 > in.writerIndex()) {
          break;
        }

        if (in.getByte(boundaryPos + 1) != '-') {
          pos = boundaryPos + 1;
          continue;
        }

        if (startIndex != boundaryPos && (boundaryPos - 2 < startIndex || !isCRLF(in, boundaryPos - 2))
            || !ByteBufUtil.equals(in, boundaryPos + 2, boundaryByteBuf, 0, boundary.length())) {
          pos = boundaryPos + 2;
          continue;
        }

        int boundaryEnd = boundaryPos + 2 + boundary.length();

        if (boundaryEnd + 2 > in.writerIndex()) {
          break;
        }

        boolean startBoundary = isCRLF(in, boundaryEnd);
        boolean maybeFinalBoundary =
            !startBoundary && in.getByte(boundaryEnd) == '-' && in.getByte(boundaryEnd + 1) == '-';
        boolean truncatedFinalBoundary = maybeFinalBoundary && boundaryEnd + 4 > in.writerIndex();
        boolean finalBoundary = maybeFinalBoundary && !truncatedFinalBoundary && isCRLF(in, boundaryEnd + 2);

        if (truncatedFinalBoundary) {
          break;
        }

        if (startBoundary || finalBoundary) {
          if (passThroughRemaining(ctx, in, boundaryPos, out)) {
            // nope, it was bogus boundary inside the multipart content
            pos = in.readerIndex();
            continue;
          }
          if (startBoundary) {
            pos = in.readerIndex(boundaryEnd + 2).readerIndex();
          } else {
            pos = in.readerIndex(boundaryEnd + 4).readerIndex();
            _inPart = false;
            continue;
          }
          super.decode(ctx, DUMMY_HEADER.duplicate(), out);
          assert State.READ_HEADER == getDecoderState() : "state: " + getDecoderState();
          _inPart = true;
          continue;
        }

        pos = boundaryEnd;
      }
      if (_decodeLast > 0 && in.isReadable()) {
        if (passThroughRemaining(ctx, in, in.writerIndex(), out)) {
          last(ctx, Unpooled.EMPTY_BUFFER, out);
        }
        _lastReadable = in.readableBytes();
      } else {
        _lastReadable = in.writerIndex() - pos;
      }
    } else {
      out.add(in.readRetainedSlice(in.readableBytes()));
      _lastReadable = 0;
    }
  }

  @Override
  protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    boolean inPart = _inPart;
    if (inPart) {
      passThroughRemaining(ctx, in, in.writerIndex(), out);
      _lastReadable = 0;
    }
    try {
      _decodeLast++;
      super.decodeLast(ctx, in, out);
    } finally {
      _decodeLast--;
      internalBuffer().clear();
    }
  }

  private void last(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    try {
      _innerDecodeLast++;
      super.decodeLast(ctx, in, out);
    } finally {
      _innerDecodeLast--;
    }
  }

  @Override
  protected boolean isDecodingRequest() {
    return false;
  }

  @Override
  protected HttpMessage createMessage(String[] initialLine) {
    return new Part(_unwrap._httpMessage, new DefaultHttpHeaders(validateHeaders));
  }

  @Override
  protected HttpMessage createInvalidMessage() {
    LOG.warn("Current state: {}", getDecoderState());
    throw new IllegalStateException();
  }

  private static class Part extends BasicHttpMultiPart implements HttpResponse {
    Part(HttpMessage httpMessage, DefaultHttpHeaders entries) {
      super(httpMessage, entries);
    }

    @Override
    public HttpResponseStatus getStatus() {
      return status();
    }

    @Override
    public HttpResponseStatus status() {
      return HttpResponseStatus.OK;
    }

    @Override
    public Part setStatus(HttpResponseStatus status) {
      return this;
    }

    @Override
    public Part setProtocolVersion(HttpVersion version) {
      super.setProtocolVersion(version);
      return this;
    }
  }
}
