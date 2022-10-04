package com.linkedin.alpini.netty4.http2;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handler for compacting Espresso headers to a single header entry. Only for HTTP/2 testing purposes.
 */
@ChannelHandler.Sharable
public class CompactHeaderHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LogManager.getLogger(CompactHeaderHandler.class);

  public static final String COMPACT_HEADER = "ech";
  private static final AttributeKey<Boolean> USE_COMPACT_HEADER_ATTR_KEY = AttributeKey.newInstance("useCompactHeader");

  private final Map<AsciiString, Integer> _headerMapToInt = new HashMap<>();
  private final Map<Integer, AsciiString> _headerMapToString = new HashMap<>();

  private final boolean _isServer;

  /**
   * Constructor. Uses reflection so it's expected to be initialized only once and shared.
   * @param headerClass Should espresso-pub's Header.class
   */
  public CompactHeaderHandler(Class<?> headerClass, boolean isServer) {
    _isServer = isServer;

    // TODO: Validate espresso-pub version
    Field[] fields = headerClass.getDeclaredFields();
    int i = 0;
    // IC Trace
    _headerMapToInt.put(AsciiString.cached("X-LI-R2-W-IC-1").toLowerCase(), i);
    _headerMapToString.put(i, AsciiString.cached("X-LI-R2-W-IC-1").toLowerCase());
    i++;

    for (Field f: fields) {
      try {
        AsciiString fieldName = AsciiString.cached((String) f.get(null)).toLowerCase();
        if (fieldName.startsWith("x")) {
          _headerMapToInt.put(fieldName, i);
          _headerMapToString.put(i, fieldName);
        }
        i++;
      } catch (Exception e) {
        // TODO: Handle Int
        // throw new Error(e);
      }
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpMessage) {
      HttpHeaders headers = ((HttpMessage) msg).headers();
      try {
        boolean shouldCheckCompactHeader = !_isServer || (ctx.channel().hasAttr(USE_COMPACT_HEADER_ATTR_KEY)
            && ctx.channel().attr(USE_COMPACT_HEADER_ATTR_KEY).getAndSet(null));

        // If the current client is sending compact headers, write compact headers back
        if (shouldCheckCompactHeader && !headers.contains(COMPACT_HEADER)) {
          ByteArrayOutputStream bo = new ByteArrayOutputStream();
          ObjectOutputStream stream = new ObjectOutputStream(bo);
          Iterator<Map.Entry<CharSequence, CharSequence>> headersIt = headers.iteratorCharSequence();
          while (headersIt.hasNext()) {
            Map.Entry<CharSequence, CharSequence> header = headersIt.next();
            Integer intKey = _headerMapToInt.get(AsciiString.of(header.getKey()).toLowerCase());
            if (intKey != null) {
              stream.writeShort(intKey);
              stream.writeUTF(header.getValue().toString());
              headers.remove(header.getKey());
            }
          }
          stream.flush();
          // Replace all the headers with single compact header.
          headers.add(COMPACT_HEADER, new AsciiString(bo.toByteArray()));
          stream.reset();
        }
      } catch (Exception e) {
        LOG.error("Exception in compact header handler", e);
      }
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpMessage) {
      HttpHeaders headers = ((HttpMessage) msg).headers();
      String compactHeaderValue = headers.get(COMPACT_HEADER);
      if (compactHeaderValue != null && !compactHeaderValue.isEmpty()) {
        byte[] b = AsciiString.of(compactHeaderValue).toByteArray();
        ByteArrayInputStream bi = new ByteArrayInputStream(b);
        ObjectInputStream si = new ObjectInputStream(bi);
        try {
          while (si.available() > 0) {
            AsciiString stringKey = _headerMapToString.get(si.readUnsignedShort());
            String value = si.readUTF();
            if (stringKey != null) {
              headers.add(stringKey, value);
            }
          }
        } catch (Exception e) {
          // Do Nothing, its EOF
        }
        // Remove the compact header.
        headers.remove(COMPACT_HEADER);

        // If is server, set the USE_COMPACT_HEADER_ATTR_KEY to true to indicate that the current client is sending
        // compact headers
        if (_isServer) {
          ctx.channel().attr(USE_COMPACT_HEADER_ATTR_KEY).set(Boolean.TRUE);
        }
      }
    }
    super.channelRead(ctx, msg);
  }
}
