package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.util.AsciiString;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A simple cache to reduce the number of duplicate {@linkplain CharSequence}s held by the {@linkplain HttpHeaders}
 * instances. The reason is that the HttpObjectDecoder will instantiate a new String for every part of the header
 * that it encountered we want to facilitate the garbage collector in reaping short life objects.
 *
 * The implementation uses a queue of maps per thread instead of maintaining a LRU structure where initially two
 * maps are added to the queue and at each expire interval, a new empty map is added at the start and an old map is
 * removed from the end. This effectively makes the lifetime of every map to be 2x the expire interval and entries
 * referenced would be inserted into only the map at the head of the queue.
 */
@ChannelHandler.Sharable
public class ElideDuplicateHeadersHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LogManager.getLogger(ElideDuplicateHeadersHandler.class);

  public static final ChannelDuplexHandler INSTANCE = new ElideDuplicateHeadersHandler();

  private static final int EXPIRE_MILLISECONDS = 1000; // A + B Map == 2 seconds retention

  private static final ThreadLocal<Cache> LOCAL = ThreadLocal.withInitial(Cache::new);

  private static class Cache {
    private final Deque<Map<AsciiString, AsciiString>> _cache = new ArrayDeque<>(2);
    private long _expirationTime;

    Cache() {
      _cache.add(new HashMap<>());
      _cache.add(new HashMap<>());
    }

    AsciiString dedup(AsciiString source) {
      return _cache.peekFirst().computeIfAbsent(source, s -> _cache.peekLast().getOrDefault(s, s));
    }

    void expire() {
      long now = Time.nanoTime();
      if (now > _expirationTime) {
        _cache.addFirst(new HashMap<>());
        int removedSize = _cache.removeLast().size();
        LOG.debug("removing old map (size={})", removedSize);
        _expirationTime = now + TimeUnit.MILLISECONDS.toNanos(EXPIRE_MILLISECONDS);
      }
    }

    void elideDuplicate(HttpHeaders headers) {
      List<Map.Entry<AsciiString, AsciiString>> entries = new ArrayList<>(headers.size());
      for (Iterator<Map.Entry<CharSequence, CharSequence>> it = headers.iteratorCharSequence(); it.hasNext();) {
        Map.Entry<CharSequence, CharSequence> entry = it.next();
        entries.add(
            new AbstractMap.SimpleImmutableEntry<>(
                dedup(AsciiString.of(entry.getKey())),
                dedup(AsciiString.of(entry.getValue()))));
      }
      headers.clear();
      entries.forEach(entry -> headers.add(entry.getKey(), entry.getValue()));
    }
  }

  private ElideDuplicateHeadersHandler() {
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpMessage) {
      Cache cache = LOCAL.get();
      cache.elideDuplicate(((HttpMessage) msg).headers());
      cache.expire();
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpMessage) {
      Cache cache = LOCAL.get();
      cache.elideDuplicate(((HttpMessage) msg).headers());
      cache.expire();
    }
    super.channelRead(ctx, msg);
  }
}
