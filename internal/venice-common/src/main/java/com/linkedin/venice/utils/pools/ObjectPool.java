package com.linkedin.venice.utils.pools;

public interface ObjectPool<O> {
  O get();

  void dispose(O object);
}
