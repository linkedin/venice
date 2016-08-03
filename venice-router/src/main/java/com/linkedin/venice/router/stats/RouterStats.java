package com.linkedin.venice.router.stats;

public interface RouterStats {
  //thread-safe is done at Tehuti level
  //TODO: we should clssify unhealthRequest in a more specific way. Such as timeout request, internal_error request
  void addRequest();

  void addHealthyRequest();

  void addUnhealthyRequest();

  void addLatency(double latency);

  void addKeySize(double keySize);

  void addValueSize(double valueSize);

  void close();
}
