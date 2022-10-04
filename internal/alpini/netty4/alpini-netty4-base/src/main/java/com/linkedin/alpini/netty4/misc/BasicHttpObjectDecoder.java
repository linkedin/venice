package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.ByteBufAsciiString;
import com.linkedin.alpini.base.misc.HeaderStringCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpConstants;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpExpectationFailedEvent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ByteProcessor;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class BasicHttpObjectDecoder extends HttpObjectDecoder {
  private static final Logger LOG = LogManager.getLogger(BasicHttpObjectDecoder.class);

  private final int maxChunkSize;
  private final boolean chunkedSupported;
  protected final boolean validateHeaders;
  private final HeaderParser headerParser;
  private final LineParser lineParser;

  private HttpMessage message;
  private long chunkSize;
  private long contentLength = Long.MIN_VALUE;
  private volatile boolean resetRequested;

  // These will be updated by splitHeader(...)
  private ByteBufAsciiString name;
  private ByteBufAsciiString value;

  private LastHttpContent trailer;

  /**
   * The internal state of {@link BasicHttpObjectDecoder}.
   * <em>Internal use only</em>.
   */
  public enum State {
    SKIP_CONTROL_CHARS, READ_INITIAL, READ_HEADER, READ_VARIABLE_LENGTH_CONTENT, READ_FIXED_LENGTH_CONTENT,
    READ_CHUNK_SIZE, READ_CHUNKED_CONTENT, READ_CHUNK_DELIMITER, READ_CHUNK_FOOTER, BAD_MESSAGE, UPGRADED
  }

  private State currentState = State.SKIP_CONTROL_CHARS;

  /**
   * Creates a new instance with the default
   * {@code maxInitialLineLength (4096}}, {@code maxHeaderSize (8192)}, and
   * {@code maxChunkSize (8192)}.
   */
  protected BasicHttpObjectDecoder() {
    this(4096, 8192, 8192, true);
  }

  /**
   * Creates a new instance with the specified parameters.
   */
  protected BasicHttpObjectDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean chunkedSupported) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, chunkedSupported, true);
  }

  /**
   * Creates a new instance with the specified parameters.
   */
  protected BasicHttpObjectDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean chunkedSupported,
      boolean validateHeaders) {
    this(maxInitialLineLength, maxHeaderSize, maxChunkSize, chunkedSupported, validateHeaders, 128);
  }

  protected BasicHttpObjectDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean chunkedSupported,
      boolean validateHeaders,
      int initialBufferSize) {
    if (maxInitialLineLength <= 0) {
      throw new IllegalArgumentException("maxInitialLineLength must be a positive integer: " + maxInitialLineLength);
    }
    if (maxHeaderSize <= 0) {
      throw new IllegalArgumentException("maxHeaderSize must be a positive integer: " + maxHeaderSize);
    }
    if (maxChunkSize <= 0) {
      throw new IllegalArgumentException("maxChunkSize must be a positive integer: " + maxChunkSize);
    }
    lineParser = new LineParser(maxInitialLineLength);
    headerParser = new HeaderParser(maxHeaderSize);
    this.maxChunkSize = maxChunkSize;
    this.chunkedSupported = chunkedSupported;
    this.validateHeaders = validateHeaders;
  }

  public final State getDecoderState() {
    return currentState;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
    if (resetRequested) {
      resetNow();
    }

    switch (currentState) {
      case SKIP_CONTROL_CHARS: {
        if (!skipControlCharacters(buffer)) {
          return;
        }
        currentState = State.READ_INITIAL;
      }
      case READ_INITIAL:
        try { // SUPPRESS CHECKSTYLE FallThroughCheck
          ByteBufAsciiString line = lineParser.parse(buffer);
          if (line == null) {
            return;
          }
          ByteBufAsciiString[] initialLine = splitInitialLine(line);
          if (initialLine.length < 3) {
            // Invalid initial line - ignore.
            currentState = State.SKIP_CONTROL_CHARS;
            return;
          }

          try {
            message = createMessage(initialLine);
            currentState = State.READ_HEADER;
          } finally {
            line.release();
          }
          // fall-through
        } catch (Exception e) {
          out.add(invalidMessage(buffer, e));
          return;
        }
      case READ_HEADER:
        try { // SUPPRESS CHECKSTYLE FallThroughCheck
          State nextState = readHeaders(buffer);
          if (nextState == null) {
            return;
          }
          currentState = nextState;
          switch (nextState) {
            case SKIP_CONTROL_CHARS:
              // fast-path
              // No content is expected.
              out.add(message);
              out.add(LastHttpContent.EMPTY_LAST_CONTENT);
              resetNow();
              return;
            case READ_CHUNK_SIZE:
              if (!chunkedSupported) {
                throw new IllegalArgumentException("Chunked messages not supported");
              }
              // Chunked encoding - generate HttpMessage first. HttpChunks will follow.
              out.add(message);
              return;
            default:
              /*
               * <a href="https://tools.ietf.org/html/rfc7230#section-3.3.3">RFC 7230, 3.3.3</a> states that if a
               * request does not have either a transfer-encoding or a content-length header then the message body
               * length is 0. However for a response the body length is the number of octets received prior to the
               * server closing the connection. So we treat this as variable length chunked encoding.
               */
              long contentLength = contentLength();
              if (contentLength == 0 || contentLength == -1 && isDecodingRequest()) {
                out.add(message);
                out.add(LastHttpContent.EMPTY_LAST_CONTENT);
                resetNow();
                return;
              }

              assert nextState == State.READ_FIXED_LENGTH_CONTENT || nextState == State.READ_VARIABLE_LENGTH_CONTENT;

              out.add(message);

              if (nextState == State.READ_FIXED_LENGTH_CONTENT) {
                // chunkSize will be decreased as the READ_FIXED_LENGTH_CONTENT state reads data chunk by chunk.
                chunkSize = contentLength;
              }

              // We return here, this forces decode to be called again where we will decode the content
              return;
          }
        } catch (Exception e) {
          out.add(invalidMessage(buffer, e));
          return;
        }
      case READ_VARIABLE_LENGTH_CONTENT: {
        // Keep reading data as a chunk until the end of connection is reached.
        int toRead = Math.min(buffer.readableBytes(), maxChunkSize);
        if (toRead > 0) {
          ByteBuf content = buffer.readRetainedSlice(toRead);
          out.add(new DefaultHttpContent(content));
        }
        return;
      }
      case READ_FIXED_LENGTH_CONTENT: {
        int readLimit = buffer.readableBytes();

        // Check if the buffer is readable first as we use the readable byte count
        // to create the HttpChunk. This is needed as otherwise we may end up with
        // create a HttpChunk instance that contains an empty buffer and so is
        // handled like it is the last HttpChunk.
        //
        // See https://github.com/netty/netty/issues/433
        if (readLimit == 0) {
          return;
        }

        int toRead = Math.min(readLimit, maxChunkSize);
        if (toRead > chunkSize) {
          toRead = (int) chunkSize;
        }
        ByteBuf content = buffer.readRetainedSlice(toRead);
        chunkSize -= toRead;

        if (chunkSize == 0) {
          // Read all content.
          out.add(new DefaultLastHttpContent(content, validateHeaders));
          resetNow();
        } else {
          out.add(new DefaultHttpContent(content));
        }
        return;
      }
      /*
       * everything else after this point takes care of reading chunked content. basically, read chunk size,
       * read chunk, read and ignore the CRLF and repeat until 0
       */
      case READ_CHUNK_SIZE:
        try {
          ByteBufAsciiString line = lineParser.parse(buffer);
          if (line == null) {
            return;
          }
          int chunkSize;
          try {
            chunkSize = getChunkSize(line);
          } finally {
            line.release();
          }
          this.chunkSize = chunkSize;
          if (chunkSize == 0) {
            currentState = State.READ_CHUNK_FOOTER;
            return;
          }
          currentState = State.READ_CHUNKED_CONTENT;
          // fall-through
        } catch (Exception e) {
          out.add(invalidChunk(buffer, e));
          return;
        }
      case READ_CHUNKED_CONTENT: { // SUPPRESS CHECKSTYLE FallThroughCheck
        assert chunkSize <= Integer.MAX_VALUE;
        int toRead = Math.min((int) chunkSize, maxChunkSize);
        toRead = Math.min(toRead, buffer.readableBytes());
        if (toRead == 0) {
          return;
        }
        HttpContent chunk = new DefaultHttpContent(buffer.readRetainedSlice(toRead));
        chunkSize -= toRead;

        out.add(chunk);

        if (chunkSize != 0) {
          return;
        }
        currentState = State.READ_CHUNK_DELIMITER;
        // fall-through
      }
      case READ_CHUNK_DELIMITER: { // SUPPRESS CHECKSTYLE FallThroughCheck
        final int wIdx = buffer.writerIndex();
        int rIdx = buffer.readerIndex();
        while (wIdx > rIdx) {
          byte next = buffer.getByte(rIdx++);
          if (next == HttpConstants.LF) {
            currentState = State.READ_CHUNK_SIZE;
            break;
          }
        }
        buffer.readerIndex(rIdx);
        return;
      }
      case READ_CHUNK_FOOTER:
        try {
          LastHttpContent trailer = readTrailingHeaders(buffer);
          if (trailer == null) {
            return;
          }
          out.add(trailer);
          resetNow();
          return;
        } catch (Exception e) {
          out.add(invalidChunk(buffer, e));
          return;
        }
      case BAD_MESSAGE: {
        // Keep discarding until disconnection.
        buffer.skipBytes(buffer.readableBytes());
        break;
      }
      case UPGRADED: {
        int readableBytes = buffer.readableBytes();
        if (readableBytes > 0) {
          // Keep on consuming as otherwise we may trigger an DecoderException,
          // other handler will replace this codec with the upgraded protocol codec to
          // take the traffic over at some point then.
          // See https://github.com/netty/netty/issues/2173
          out.add(buffer.readBytes(readableBytes));
        }
        break;
      }
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    super.decodeLast(ctx, in, out);

    if (resetRequested) {
      // If a reset was requested by decodeLast() we need to do it now otherwise we may produce a
      // LastHttpContent while there was already one.
      resetNow();
    }
    // Handle the last unfinished message.
    if (message != null) {
      boolean chunked = HttpUtil.isTransferEncodingChunked(message);
      if (currentState == State.READ_VARIABLE_LENGTH_CONTENT && !in.isReadable() && !chunked) {
        // End of connection.
        out.add(LastHttpContent.EMPTY_LAST_CONTENT);
        resetNow();
        return;
      }

      if (currentState == State.READ_HEADER) {
        // If we are still in the state of reading headers we need to create a new invalid message that
        // signals that the connection was closed before we received the headers.
        out.add(
            invalidMessage(
                Unpooled.EMPTY_BUFFER,
                new PrematureChannelClosureException("Connection closed before received headers")));
        resetNow();
        return;
      }

      // Check if the closure of the connection signifies the end of the content.
      boolean prematureClosure;
      if (isDecodingRequest() || chunked) {
        // The last request did not wait for a response.
        prematureClosure = true;
      } else {
        // Compare the length of the received content and the 'Content-Length' header.
        // If the 'Content-Length' header is absent, the length of the content is determined by the end of the
        // connection, so it is perfectly fine.
        prematureClosure = contentLength() > 0;
      }

      if (!prematureClosure) {
        out.add(LastHttpContent.EMPTY_LAST_CONTENT);
      }
      resetNow();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof HttpExpectationFailedEvent) {
      switch (currentState) {
        case READ_FIXED_LENGTH_CONTENT:
        case READ_VARIABLE_LENGTH_CONTENT:
        case READ_CHUNK_SIZE:
          reset();
          break;
        default:
          break;
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  protected boolean isContentAlwaysEmpty(HttpMessage msg) {
    if (msg instanceof HttpResponse) {
      HttpResponse res = (HttpResponse) msg;
      int code = res.status().code();

      // Correctly handle return codes of 1xx.
      //
      // See:
      // - http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html Section 4.4
      // - https://github.com/netty/netty/issues/222
      if (code >= 100 && code < 200) {
        // One exception: Hixie 76 websocket handshake response
        return !(code == 101 && !res.headers().contains(HttpHeaderNames.SEC_WEBSOCKET_ACCEPT)
            && res.headers().contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true));
      }

      switch (code) {
        case 204:
        case 304:
          return true;
        default:
          break;
      }
    }
    return false;
  }

  /**
   * Returns true if the server switched to a different protocol than HTTP/1.0 or HTTP/1.1, e.g. HTTP/2 or Websocket.
   * Returns false if the upgrade happened in a different layer, e.g. upgrade from HTTP/1.1 to HTTP/1.1 over TLS.
   */
  protected boolean isSwitchingToNonHttp1Protocol(HttpResponse msg) {
    if (msg.status().code() != HttpResponseStatus.SWITCHING_PROTOCOLS.code()) {
      return false;
    }
    String newProtocol = msg.headers().get(HttpHeaderNames.UPGRADE);
    return newProtocol == null
        || !newProtocol.contains(HttpVersion.HTTP_1_0.text()) && !newProtocol.contains(HttpVersion.HTTP_1_1.text());
  }

  /**
   * Resets the state of the decoder so that it is ready to decode a new message.
   * This method is useful for handling a rejected request with {@code Expect: 100-continue} header.
   */
  public void reset() {
    resetRequested = true;
  }

  private void resetHeaderName() {
    if (name != null) {
      name.release();
    }
    if (value != null) {
      value.release();
    }
    name = null;
    value = null;
  }

  private void resetNow() {
    HttpMessage message = this.message;
    this.message = null;
    resetHeaderName();
    contentLength = Long.MIN_VALUE;
    lineParser.reset();
    headerParser.reset();
    trailer = null;
    if (!isDecodingRequest()) {
      HttpResponse res = (HttpResponse) message;
      if (res != null && isSwitchingToNonHttp1Protocol(res)) {
        currentState = State.UPGRADED;
        return;
      }
    }

    resetRequested = false;
    currentState = State.SKIP_CONTROL_CHARS;
  }

  private HttpMessage invalidMessage(ByteBuf in, Exception cause) {
    LOG.error("invalidMessage", cause);
    currentState = State.BAD_MESSAGE;

    // Advance the readerIndex so that ByteToMessageDecoder does not complain
    // when we produced an invalid message without consuming anything.
    in.skipBytes(in.readableBytes());

    if (message == null) {
      message = createInvalidMessage();
    }
    message.setDecoderResult(DecoderResult.failure(cause));

    HttpMessage ret = message;
    message = null;
    return ret;
  }

  private HttpContent invalidChunk(ByteBuf in, Exception cause) {
    LOG.error("invalidChunk", cause);
    currentState = State.BAD_MESSAGE;

    // Advance the readerIndex so that ByteToMessageDecoder does not complain
    // when we produced an invalid message without consuming anything.
    in.skipBytes(in.readableBytes());

    HttpContent chunk = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
    chunk.setDecoderResult(DecoderResult.failure(cause));
    message = null;
    trailer = null;
    return chunk;
  }

  private static boolean skipControlCharacters(ByteBuf buffer) {
    boolean skiped = false;
    final int wIdx = buffer.writerIndex();
    int rIdx = buffer.readerIndex();
    while (wIdx > rIdx) {
      int c = buffer.getUnsignedByte(rIdx++);
      if (!Character.isISOControl(c) && !Character.isWhitespace(c)) {
        rIdx--;
        skiped = true;
        break;
      }
    }
    buffer.readerIndex(rIdx);
    return skiped;
  }

  private static void addHeader(
      HeaderStringCache.Cache cache,
      HttpHeaders headers,
      CharSequence name,
      CharSequence value) {
    name = cache.lookupName(name);
    value = cache.lookupValue(value);
    headers.add(name, value);
  }

  private State readHeaders(ByteBuf buffer) {
    final HttpMessage message = this.message;
    final HttpHeaders headers = message.headers();
    final HeaderStringCache.Cache cache = HeaderStringCache.getAndExpireOld();

    ByteBufAsciiString line = headerParser.parse(buffer);
    if (line == null) {
      return null;
    }
    try {
      if (line.length() > 0) {
        do {
          char firstChar = line.charAt(0);
          if (name != null && (firstChar == ' ' || firstChar == '\t')) {
            ByteBufAsciiString oldValue = value;

            value = oldValue.concat(" ", line.trim());
            oldValue.release();
          } else {
            if (name != null) {
              addHeader(cache, headers, name, value);
            }
            splitHeader(line);
          }

          ByteBufAsciiString newLine = headerParser.parse(buffer);
          if (newLine == null) {
            return null;
          }
          ByteBufAsciiString oldLine = line;
          line = newLine;
          oldLine.release();
        } while (line.length() > 0);
      }
    } finally {
      line.release();
    }

    // Add the last header.
    if (name != null) {
      addHeader(cache, headers, name, value);
    }
    // reset name and value fields
    resetHeaderName();

    State nextState;

    if (isContentAlwaysEmpty(message)) {
      HttpUtil.setTransferEncodingChunked(message, false);
      nextState = State.SKIP_CONTROL_CHARS;
    } else if (HttpUtil.isTransferEncodingChunked(message)) {
      nextState = State.READ_CHUNK_SIZE;
    } else if (contentLength() >= 0) {
      nextState = State.READ_FIXED_LENGTH_CONTENT;
    } else {
      nextState = State.READ_VARIABLE_LENGTH_CONTENT;
    }
    return nextState;
  }

  private long contentLength() {
    if (contentLength == Long.MIN_VALUE) {
      contentLength = HttpUtil.getContentLength(message, -1L);
    }
    return contentLength;
  }

  private LastHttpContent readTrailingHeaders(ByteBuf buffer) {
    ByteBufAsciiString line = headerParser.parse(buffer);
    if (line == null) {
      return null;
    }
    try {
      ByteBufAsciiString lastHeader = null;
      if (line.length() > 0) {
        HeaderStringCache.Cache cache = HeaderStringCache.getAndExpireOld();
        LastHttpContent trailer = this.trailer;
        if (trailer == null) {
          trailer = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, validateHeaders);
          this.trailer = trailer;
        }
        do {
          char firstChar = line.charAt(0);
          if (lastHeader != null && (firstChar == ' ' || firstChar == '\t')) {
            List<String> current = trailer.trailingHeaders().getAll(lastHeader);
            if (!current.isEmpty()) {
              int lastPos = current.size() - 1;
              // please do not make one line from below code
              // as it breaks +XX:OptimizeStringConcat optimization
              ByteBufAsciiString lineTrimmed = line.trim();
              String currentLastPos = current.get(lastPos);
              current.set(lastPos, currentLastPos + lineTrimmed);
            }
          } else {
            splitHeader(line);
            ByteBufAsciiString headerName = name;
            if (!HttpHeaderNames.CONTENT_LENGTH.contentEqualsIgnoreCase(headerName)
                && !HttpHeaderNames.TRANSFER_ENCODING.contentEqualsIgnoreCase(headerName)
                && !HttpHeaderNames.TRAILER.contentEqualsIgnoreCase(headerName)) {
              addHeader(cache, trailer.trailingHeaders(), headerName, value);
            }
            lastHeader = name;
            // reset name and value fields
            resetHeaderName();
          }

          ByteBufAsciiString newLine = headerParser.parse(buffer);
          if (newLine == null) {
            return null;
          }
          ByteBufAsciiString oldLine = line;
          line = newLine;
          oldLine.release();
        } while (line.length() > 0);

        this.trailer = null;
        return trailer;
      }
    } finally {
      line.release();
    }

    return LastHttpContent.EMPTY_LAST_CONTENT;
  }

  protected abstract boolean isDecodingRequest();

  protected HttpMessage createMessage(ByteBufAsciiString[] initialLine) throws Exception {
    return createMessage(
        new String[] { initialLine[0].toString(), initialLine[1].toString(), initialLine[2].toString() });
  }

  protected abstract HttpMessage createMessage(String[] initialLine) throws Exception;

  protected abstract HttpMessage createInvalidMessage();

  private static int getChunkSize(ByteBufAsciiString hex) {
    hex = hex.trim();
    for (int i = 0; i < hex.length(); i++) {
      char c = hex.charAt(i);
      if (c == ';' || Character.isWhitespace(c) || Character.isISOControl(c)) {
        hex = hex.subSequence(0, i);
        break;
      }
    }

    return Integer.parseUnsignedInt(hex.toString(), 16);
  }

  private static ByteBufAsciiString[] splitInitialLine(ByteBufAsciiString sb) {
    int aStart;
    int aEnd;
    int bStart;
    int bEnd;
    int cStart;
    int cEnd;

    aStart = findNonWhitespace(sb, 0);
    aEnd = findWhitespace(sb, aStart);

    bStart = findNonWhitespace(sb, aEnd);
    bEnd = findWhitespace(sb, bStart);

    cStart = findNonWhitespace(sb, bEnd);
    cEnd = findEndOfString(sb);

    return new ByteBufAsciiString[] { sb.subSequence(aStart, aEnd, false), sb.subSequence(bStart, bEnd, false),
        cStart < cEnd ? sb.subSequence(cStart, cEnd, false) : ByteBufAsciiString.EMPTY_STRING };
  }

  private void splitHeader(ByteBufAsciiString sb) {
    final int length = sb.length();
    int nameStart;
    int nameEnd;
    int colonEnd;
    int valueStart;
    int valueEnd;

    nameStart = findNonWhitespace(sb, 0);
    for (nameEnd = nameStart; nameEnd < length; nameEnd++) {
      char ch = sb.charAt(nameEnd);
      if (ch == ':' || Character.isWhitespace(ch)) {
        break;
      }
    }

    for (colonEnd = nameEnd; colonEnd < length; colonEnd++) {
      if (sb.charAt(colonEnd) == ':') {
        colonEnd++;
        break;
      }
    }

    resetHeaderName();

    name = sb.subSequence(nameStart, nameEnd, false);
    valueStart = findNonWhitespace(sb, colonEnd);
    if (valueStart == length) {
      value = ByteBufAsciiString.EMPTY_STRING;
    } else {
      valueEnd = findEndOfString(sb);
      value = sb.subSequence(valueStart, valueEnd, false);
    }

    name.retain();
    value.retain();
  }

  private static int findNonWhitespace(ByteBufAsciiString sb, int offset) {
    for (int result = offset; result < sb.length(); ++result) {
      if (!Character.isWhitespace(sb.charAt(result))) {
        return result;
      }
    }
    return sb.length();
  }

  private static int findWhitespace(ByteBufAsciiString sb, int offset) {
    for (int result = offset; result < sb.length(); ++result) {
      if (Character.isWhitespace(sb.charAt(result))) {
        return result;
      }
    }
    return sb.length();
  }

  private static int findEndOfString(ByteBufAsciiString sb) {
    for (int result = sb.length() - 1; result > 0; --result) {
      if (!Character.isWhitespace(sb.charAt(result))) {
        return result + 1;
      }
    }
    return 0;
  }

  private static class HeaderParser implements ByteProcessor {
    private final int maxLength;
    private int size;

    HeaderParser(int maxLength) {
      this.maxLength = maxLength;
    }

    public ByteBufAsciiString parse(ByteBuf buffer) {
      final int oldSize = size;
      int start = buffer.readerIndex();
      int i = buffer.forEachByte(this);
      if (i == -1) {
        size = oldSize;
        return null;
      }
      int length = i - start;
      while (length > 0 && buffer.getByte(start + length - 1) == HttpConstants.CR) {
        length--;
      }
      ByteBufAsciiString result = ByteBufAsciiString.read(buffer, length);
      buffer.readerIndex(i + 1);
      return result;
    }

    public void reset() {
      size = 0;
    }

    @Override
    public boolean process(byte value) {
      char nextByte = (char) (value & 0xFF);
      if (nextByte == HttpConstants.CR) {
        return true;
      }
      if (nextByte == HttpConstants.LF) {
        return false;
      }

      if (++size > maxLength) {
        // TODO: Respond with Bad Request and discard the traffic
        // or close the connection.
        // No need to notify the upstream handlers - just log.
        // If decoding a response, just throw an exception.
        throw newException(maxLength);
      }
      return true;
    }

    protected TooLongFrameException newException(int maxLength) {
      return new TooLongFrameException("HTTP header is larger than " + maxLength + " bytes.");
    }
  }

  private static final class LineParser extends HeaderParser {
    LineParser(int maxLength) {
      super(maxLength);
    }

    @Override
    public ByteBufAsciiString parse(ByteBuf buffer) {
      reset();
      return super.parse(buffer);
    }

    @Override
    protected TooLongFrameException newException(int maxLength) {
      return new TooLongFrameException("An HTTP line is larger than " + maxLength + " bytes.");
    }
  }
}
