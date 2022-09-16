package com.linkedin.venice.utils;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;


/**
 * Created by mwise on 4/12/16.
 */
public class ExpiringSet<T> {
  // Map from element to the expiration time for that element
  private Map<T, Long> internalMap;
  private long millisTTL;

  public ExpiringSet(long ttl, TimeUnit unit) {
    internalMap = new VeniceConcurrentHashMap<>();
    this.millisTTL = unit.toMillis(ttl);
  }

  public void add(T element) {
    internalMap.put(element, System.currentTimeMillis() + millisTTL);
  }

  public void add(T element, long ttl, TimeUnit unit) {
    internalMap.put(element, System.currentTimeMillis() + unit.toMillis(ttl));
  }

  public void remove(T element) {
    internalMap.remove(element);
  }

  public boolean contains(T element) {
    Long expireTime = internalMap.computeIfPresent(element, new BiFunction<T, Long, Long>() {
      @Override
      public Long apply(T elem, Long expiration) { /* occurs atomically, no need to synchronize */
        if (expiration > System.currentTimeMillis()) {
          return expiration;
        } else {
          return null;
        }
      }
    });

    if (expireTime == null) {
      return false;
    } else {
      return true;
    }
  }
}
