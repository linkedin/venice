package com.linkedin.alpini.log;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


public class FastLog implements FastLogMBean {
  private static final AtomicReferenceFieldUpdater<FastLog, Boolean> DEBUG_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FastLog.class, Boolean.class, "_debugEnabled");

  private static final AtomicReferenceFieldUpdater<FastLog, Boolean> TRACE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(FastLog.class, Boolean.class, "_traceEnabled");

  private volatile Boolean _debugEnabled = false;
  private volatile Boolean _traceEnabled = false;

  public boolean isDebugEnabled() {
    return _debugEnabled;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }

  @Override
  public void setDebug(boolean enabled) {
    DEBUG_UPDATER.lazySet(this, enabled);
  }

  @Override
  public void setTrace(boolean enabled) {
    TRACE_UPDATER.lazySet(this, enabled);
  }
}
