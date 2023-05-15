package com.linkedin.alpini.router.api;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Routing information for part of a request which the router has mapped to a single storage node.
 * The ScatterGatherRequest holds a storage node hostname, plus keys and partitions which have been mapped to
 * that host. A single request to the router may be split into multiple ScatterGatherRequest objects, one for
 * each host involved in servicing the request.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 */
public class ScatterGatherRequest<H, K> {
  private final List<H> _host; // TODO Make this a single instance since we always use it with just one item
  private final @Nonnull Set<K> _keys;

  public ScatterGatherRequest(List<H> host, Set<K> partitionKeys) {
    _host = host;
    _keys = Objects.requireNonNull(partitionKeys, "partitionKeys");
  }

  public List<H> getHosts() {
    return _host;
  }

  public void removeHost(@Nonnull H host) {
    if (_host != null && (_host.remove(host) && _host.isEmpty())) {
      throw new IllegalStateException("Scatter gather request with no hosts is not allowed");
    }
  }

  public Set<K> getPartitionKeys() {
    return _keys;
  }

  /** Only used by broadcast queries. See {@link BroadcastScatterGatherRequest}. */
  public Set<String> getPartitionNamesToQuery() {
    return Collections.emptySet();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_host == null) ? 0 : _host.hashCode());
    result = prime * result + _keys.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ScatterGatherRequest other = (ScatterGatherRequest) obj;
    if (_host == null) {
      if (other._host != null) {
        return false;
      }
    } else if (!_host.equals(other._host)) {
      return false;
    }
    return _keys.equals(other._keys);
  }

  @Override
  public String toString() {
    return "host=(" + _host + ") keys=(" + _keys + ")";
  }
}
