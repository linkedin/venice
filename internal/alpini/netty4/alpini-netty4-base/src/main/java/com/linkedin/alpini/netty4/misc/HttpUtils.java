package com.linkedin.alpini.netty4.misc;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import javax.annotation.Nonnull;


/**
 * This will be used to stash reoccurring common bits of code.
 */
public enum HttpUtils {
  SINGLETON;

  /**
   * Returns a deap copy of the passed in {@link HttpHeaders}.
   */
  public static HttpHeaders copy(@Nonnull HttpHeaders src) {
    return new DefaultHttpHeaders(false).set(src);
  }
}
