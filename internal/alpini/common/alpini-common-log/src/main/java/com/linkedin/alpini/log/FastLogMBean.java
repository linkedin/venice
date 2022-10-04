package com.linkedin.alpini.log;

public interface FastLogMBean {
  boolean isDebugEnabled();

  boolean isTraceEnabled();

  void setDebug(boolean enabled);

  void setTrace(boolean enabled);
}
