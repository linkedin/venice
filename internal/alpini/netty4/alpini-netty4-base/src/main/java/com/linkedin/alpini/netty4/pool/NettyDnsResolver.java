package com.linkedin.alpini.netty4.pool;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.NameResolver;
import io.netty.resolver.dns.DefaultDnsCache;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsCache;
import io.netty.resolver.dns.DnsCacheEntry;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.DnsServerAddressStreamProvider;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/30/17.
 */
public class NettyDnsResolver implements ChannelPoolResolver {
  private static final int MIN_TTL =
      Integer.parseUnsignedInt(System.getProperty("com.linkedin.alpini.netty4.pool.dnsTtlMin", "0"));

  private static final int MAX_TTL =
      Integer.parseUnsignedInt(System.getProperty("com.linkedin.alpini.netty4.pool.dnsTtlMax", "" + Integer.MAX_VALUE));

  private static final int NEG_TTL =
      Integer.parseUnsignedInt(System.getProperty("com.linkedin.alpini.netty4.pool.dnsTtlNeg", "0"));

  private final DnsCache _cache = new DefaultDnsCache(MIN_TTL, MAX_TTL, NEG_TTL);

  private static final DnsRecord[] NO_ADDITIONALS = new DnsRecord[0];

  private final DnsAddressResolverGroup _dnsResolverGroup;

  private final EventLoopGroup _eventLoopGroup;

  private final ThreadLocal<AddressResolver<InetSocketAddress>> _resolver = new ThreadLocal<>();

  public NettyDnsResolver(Class<? extends DatagramChannel> datagramChannelClass, EventLoopGroup eventLoopGroup) {
    _dnsResolverGroup =
        new DnsAddressResolverGroup(datagramChannelClass, DefaultDnsServerAddressStreamProvider.INSTANCE) {
          @Override
          protected NameResolver<InetAddress> newNameResolver(
              EventLoop eventLoop,
              ChannelFactory<? extends DatagramChannel> channelFactory,
              DnsServerAddressStreamProvider nameServerProvider) throws Exception {
            return new DnsNameResolverBuilder(eventLoop).channelFactory(channelFactory)
                .nameServerProvider(nameServerProvider)
                .resolveCache(_cache)
                .build();
          }
        };
    _eventLoopGroup = eventLoopGroup;
  }

  @Override
  @Nonnull
  public Future<InetSocketAddress> resolve(
      @Nonnull InetSocketAddress address,
      @Nonnull Promise<InetSocketAddress> promise) {
    if (address.isUnresolved() && !promise.isDone()) {

      // Check the cache first
      List<? extends DnsCacheEntry> cacheEntries = _cache.get(address.getHostString(), NO_ADDITIONALS);
      if (cacheEntries != null) {
        // Prefer IPv6 address
        for (DnsCacheEntry entry: cacheEntries) {
          if (entry.address() == null || entry.address().getAddress().length != 16) {
            continue;
          }
          return promise.setSuccess(new InetSocketAddress(entry.address(), address.getPort()));
        }

        // Otherwise, use IPv4 address
        for (DnsCacheEntry entry: cacheEntries) {
          if (entry.address() == null || entry.address().getAddress().length != 4) {
            continue;
          }
          return promise.setSuccess(new InetSocketAddress(entry.address(), address.getPort()));
        }
      }

      EventLoop eventLoop = _eventLoopGroup.next();
      Thread current = Thread.currentThread();
      if (eventLoop.inEventLoop(current)) {
        return resolve0(eventLoop, address, promise);
      } else {
        // Keep the work on the same IO Worker if possible
        for (EventExecutor executor: _eventLoopGroup) {
          if (executor.inEventLoop(current)) {
            return resolve0((EventLoop) executor, address, promise);
          }
        }

        // Must perform a context switch since the current thread is not a member of EventLoopGroup
        eventLoop.execute(() -> resolve0(eventLoop, address, promise));
        return promise;
      }
    } else {
      return promise.setSuccess(address);
    }
  }

  private Future<InetSocketAddress> resolve0(
      EventLoop eventLoop,
      InetSocketAddress address,
      Promise<InetSocketAddress> promise) {
    return Optional.ofNullable(_resolver.get()).orElseGet(() -> {
      AddressResolver<InetSocketAddress> resolver = _dnsResolverGroup.getResolver(eventLoop);
      _resolver.set(resolver);
      return resolver;
    }).resolve(address, promise);
  }

  public DnsAddressResolverGroup getAddressResolverGroup() {
    return _dnsResolverGroup;
  }
}
