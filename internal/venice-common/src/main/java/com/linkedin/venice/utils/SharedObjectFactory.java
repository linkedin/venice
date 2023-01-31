package com.linkedin.venice.utils;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class SharedObjectFactory<T> {
  private final Map<String, ReferenceCounted<T>> objectMap = new VeniceConcurrentHashMap<>();

  public T getObject(String objectIdentifier, Supplier<T> objectSupplier, Consumer<T> destroyer) {
    if (!objectMap.containsKey(objectIdentifier)) {
      T object = objectSupplier.get();
      objectMap.put(objectIdentifier, new ReferenceCounted<>(object, obj -> {
        destroyer.accept(obj);
        objectMap.remove(objectIdentifier);
      }));
      return object;
    } else {
      ReferenceCounted<T> referenceCounted = objectMap.get(objectIdentifier);
      referenceCounted.retain();
      return referenceCounted.get();
    }
  }

  public void release(String objectIdentifier) {
    ReferenceCounted<T> referenceCounted = objectMap.get(objectIdentifier);
    if (referenceCounted != null) {
      referenceCounted.release();
    }
  }

  public int size() {
    return objectMap.size();
  }

  public int getReferenceCount(String objectIdentifier) {
    if (!objectMap.containsKey(objectIdentifier)) {
      return 0;
    } else {
      return objectMap.get(objectIdentifier).getReferenceCount();
    }
  }
}
