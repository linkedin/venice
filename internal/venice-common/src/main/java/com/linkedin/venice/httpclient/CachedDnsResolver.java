package com.linkedin.venice.httpclient;

import com.linkedin.venice.stats.DnsLookupStats;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.conn.DnsResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link CachedDnsResolver} caches the dns entries for hosts, which matches the specified host pattern.
 * In the meantime, there is a asynchronous task running to refresh the cached entries periodically.
 *
 * The reason to have this customized {@link DnsResolver}:
 * 1. If the DNS lookup is slow, {@link org.apache.http.nio.pool.AbstractNIOConnPool#lease(Object, Object)}
 * will be blocked by the slow DNS lookup, which will cause a lot of requests being blocked since the slow
 * DNS lookup will grab the per-connection-pool lock until it is done;
 * 2. Here we choose to use a asynchronous task to refresh the cached entries is that the synchronous refresh
 * with TTL will still cause latency spike because of #1;
 */
public class CachedDnsResolver implements DnsResolver, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(CachedDnsResolver.class);

  private final String cachedHostPattern;
  private final long refreshIntervalInMs;
  private final DnsLookupStats stats;
  private final Map<String, InetAddress[]> cachedDnsEntries = new ConcurrentHashMap<>();
  private final Thread refreshThread;
  private final AtomicBoolean stopRefreshing = new AtomicBoolean();

  public CachedDnsResolver(String cachedHostPattern, long refreshIntervalInMs, DnsLookupStats stats) {
    this.cachedHostPattern = cachedHostPattern;
    this.refreshIntervalInMs = refreshIntervalInMs;
    this.stats = stats;
    refreshThread = new Thread(new DnsCacheRefreshingTask());

    refreshThread.start();
  }

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    if (cachedDnsEntries.containsKey(host)) {
      return cachedDnsEntries.get(host);
    }
    // Use system default DNS resolver
    InetAddress[] socketAddresses = systemGetAllByName(host);
    if (host.matches(cachedHostPattern)) {
      cachedDnsEntries.putIfAbsent(host, socketAddresses);
      LOGGER.info(
          "Put [host:{}, socket addresses:{}] to the cache, and cache size: {}",
          host,
          Arrays.toString(socketAddresses),
          cachedDnsEntries.size());
    }
    return socketAddresses;
  }

  // for testing purpose
  protected Optional<InetAddress[]> getEntryFromDnsCache(String host) {
    InetAddress[] socketAddresses = cachedDnsEntries.get(host);
    if (socketAddresses == null) {
      return Optional.empty();
    }
    return Optional.of(socketAddresses);
  }

  protected InetAddress[] systemGetAllByName(String host) throws UnknownHostException {
    long dnsLookupStartTs = System.currentTimeMillis();
    InetAddress[] socketAddresses = InetAddress.getAllByName(host);
    long dnsLookupLatency = System.currentTimeMillis() - dnsLookupStartTs;
    if (dnsLookupLatency > 50) {
      // Log a warn message if the fetch latency exceeds 50ms
      LOGGER.warn("System DNS resolver took {}ms to resolve host: {}", dnsLookupLatency, host);
    }
    stats.recordLookupLatency(dnsLookupLatency);
    return socketAddresses;
  }

  @Override
  public void close() throws IOException {
    stopRefreshing.set(true);
    refreshThread.interrupt();
  }

  private class DnsCacheRefreshingTask implements Runnable {
    @Override
    public void run() {
      while (!stopRefreshing.get()) {
        try {
          Thread.sleep(refreshIntervalInMs);
        } catch (InterruptedException e) {
          LOGGER.error("Sleep in DnsCacheRefreshingTask gets interrupted, will exit");
        }
        for (String host: cachedDnsEntries.keySet()) {
          if (stopRefreshing.get()) {
            break;
          }
          // Use system default DNS resolver
          try {
            InetAddress[] socketAddresses = systemGetAllByName(host);
            InetAddress[] cachedSocketAddresses = cachedDnsEntries.get(host);
            if (cachedSocketAddresses == null) {
              LOGGER.error(
                  "Get null entry from DNS cache for host: {}, which is impossible.. But DnsCacheRefreshingTask will update it by address: {}",
                  host,
                  Arrays.toString(socketAddresses));
              cachedDnsEntries.put(host, socketAddresses);
            } else if (!Arrays.equals(cachedSocketAddresses, socketAddresses)) {
              LOGGER.info(
                  "Dns entry for host: {} gets updated, previous: {}, current: {}",
                  host,
                  Arrays.toString(cachedSocketAddresses),
                  Arrays.toString(socketAddresses));
              cachedDnsEntries.put(host, socketAddresses);
            }
          } catch (UnknownHostException e) {
            LOGGER.error("Received exception when refreshing dns entry for host: {}", host, e);
          }
        }
      }
    }

  }
}
