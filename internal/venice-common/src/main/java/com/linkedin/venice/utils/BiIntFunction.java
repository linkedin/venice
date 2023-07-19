package com.linkedin.venice.utils;

public interface BiIntFunction<R> {
  R apply(int first, int second);
}
