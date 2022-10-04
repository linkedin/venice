package com.linkedin.alpini.router.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Collection of ScatterGatherRequests for a request to the router. An incoming request is broken up into
 * one or more ScatterGatherRequest, each of which represents a request which will be sent to a single storage
 * node. This class holds all of the ScatterGatherRequests for a single incoming request.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 */
public class Scatter<H, P extends ResourcePath<K>, K> {
  /** Requests for servers that are available. */
  private final Collection<ScatterGatherRequest<H, K>> _onlineRequests = new ArrayList<>();
  /** Requests for servers that are offline. */
  private final Collection<ScatterGatherRequest<H, K>> _offlineRequests = new ArrayList<>();
  private final P _path;
  private final Object _role;
  private final ResourcePathParser<P, K> _pathParser;

  public Scatter(@Nonnull P path, @Nonnull ResourcePathParser<P, K> pathParser, @Nonnull Object role) {
    _path = Objects.requireNonNull(path, "path");
    _pathParser = Objects.requireNonNull(pathParser, "pathParser");
    _role = Objects.requireNonNull(role, "role");
  }

  public void addOfflineRequest(@Nonnull ScatterGatherRequest<H, K> request) {
    _offlineRequests.add(Objects.requireNonNull(request, "request"));
  }

  public void addOnlineRequest(@Nonnull ScatterGatherRequest<H, K> request) {
    _onlineRequests.add(Objects.requireNonNull(request, "request"));
  }

  public @Nonnull Collection<ScatterGatherRequest<H, K>> getOnlineRequests() {
    return Collections.unmodifiableCollection(_onlineRequests);
  }

  public int getOnlineRequestCount() {
    return _onlineRequests.size();
  }

  public @Nonnull Collection<ScatterGatherRequest<H, K>> getOfflineRequests() {
    return Collections.unmodifiableCollection(_offlineRequests);
  }

  public int getOfflineRequestCount() {
    return _offlineRequests.size();
  }

  public @Nonnull P getPath() {
    return _path;
  }

  @SuppressWarnings("unchecked")
  public @Nonnull <R> R getRole() {
    return (R) _role;
  }

  public @Nonnull ResourcePathParser<P, K> getPathParser() {
    return _pathParser;
  }

  public @Nonnull P pathFor(@Nonnull ScatterGatherRequest<H, K> request) {
    Collection<K> keys = request.getPartitionKeys();
    if (keys.size() == 0) {
      return _pathParser.substitutePartitionKey(_path, (K) null);
    } else if (keys.size() == 1) {
      K key = keys.iterator().next();
      return _pathParser.substitutePartitionKey(_path, key);
    } else {
      return _pathParser.substitutePartitionKey(_path, keys);
    }
  }
}
