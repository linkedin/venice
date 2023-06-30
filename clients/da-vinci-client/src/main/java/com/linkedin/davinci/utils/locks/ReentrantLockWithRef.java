package com.linkedin.davinci.utils.locks;

import java.util.concurrent.locks.ReentrantLock;


public class ReentrantLockWithRef<T> extends ReentrantLock {
  private static final long serialVersionUID = -1;
  private volatile T ref = null;

  public T getRef() {
    return this.ref;
  }

  public void setRef(T ref) {
    this.ref = ref;
  }

  public void removeRef() {
    this.ref = null;
  }

  public boolean containsRef() {
    return this.ref != null;
  }
}
