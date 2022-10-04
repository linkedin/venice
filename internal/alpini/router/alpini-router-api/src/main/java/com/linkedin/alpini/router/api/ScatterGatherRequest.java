package com.linkedin.alpini.router.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
  private final List<H> _host;
  private final @Nonnull SortedSet<K> _keys;
  private final @Nonnull Set<String> _partitions;
  // client specified partitionIds intended for table level query request.
  private final @Nonnull Set<String> _partitionIdsToQuery;

  public ScatterGatherRequest(List<H> host) {
    this(host, new TreeSet<>(), new HashSet<>());
  }

  public ScatterGatherRequest(List<H> host, SortedSet<K> partitionKeys, String partitionName) {
    this(host, partitionKeys, new HashSet<>(Collections.singleton(partitionName)));
  }

  public ScatterGatherRequest(List<H> host, SortedSet<K> partitionKeys, Set<String> partitionNames) {
    this(
        host,
        Objects.requireNonNull(partitionKeys, "partitionKeys"),
        Objects.requireNonNull(partitionNames, "partitionNames"),
        new HashSet<>());
  }

  private ScatterGatherRequest(
      List<H> host,
      SortedSet<K> partitionKeys,
      Set<String> partitionNames,
      Set<String> partitionIdsToQuery) {
    _host = host;
    _keys = partitionKeys;
    _partitions = partitionNames;
    _partitionIdsToQuery = partitionIdsToQuery;
  }

  public void addKeys(Set<K> partitionKeys, String partitionName) {
    _keys.addAll(partitionKeys);
    _partitions.add(partitionName);
  }

  public void addPartitionNameToQuery(String partitionName) {
    _partitionIdsToQuery.add(Objects.requireNonNull(partitionName, "partitionName"));
  }

  public List<H> getHosts() {
    return _host;
  }

  public void removeHost(@Nonnull H host) {
    if (_host != null && (_host.remove(host) && _host.isEmpty())) {
      throw new IllegalStateException("Scatter gather request with no hosts is not allowed");
    }
  }

  public ScatterGatherRequest<H, K> substitutePartitionKeys(SortedSet<K> partitionKeys) {
    if (partitionKeys == null || partitionKeys.isEmpty()) {
      return this;
    } else {
      return new ScatterGatherRequest<H, K>(_host, partitionKeys, _partitions, _partitionIdsToQuery);
    }
  }

  public SortedSet<K> getPartitionKeys() {
    return _keys;
  }

  public Set<String> getPartitionsNames() {
    return _partitions;
  }

  /** For table level query requests, return the list of partitions that should be queried for this request. */
  public Set<String> getPartitionNamesToQuery() {
    return _partitionIdsToQuery.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(_partitionIdsToQuery);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_host == null) ? 0 : _host.hashCode());
    result = prime * result + _keys.hashCode();
    result = prime * result + _partitions.hashCode();
    result = prime * result + _partitionIdsToQuery.hashCode();
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
    return _keys.equals(other._keys) && _partitions.equals(other._partitions)
        && _partitionIdsToQuery.equals(other._partitionIdsToQuery);
  }

  @Override
  public String toString() {
    return "host=(" + _host + ") keys=(" + _keys + ") partitions=(" + _partitions + ") partitionIds=("
        + _partitionIdsToQuery + ")";
  }
}
