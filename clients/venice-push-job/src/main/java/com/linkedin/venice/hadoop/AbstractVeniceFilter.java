package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


/**
 * An abstraction to filter given data type. It can be used in conjunction with {@link FilterChain}.
 */
public abstract class AbstractVeniceFilter<INPUT_VALUE> implements Closeable {
  public AbstractVeniceFilter(final VeniceProperties props) {
  }

  /**
   * This function implements how to parse the value and determine if filtering is needed.
   * @param value
   * @return true if the value should be filtered out, otherwise false.
   */
  public abstract boolean apply(final INPUT_VALUE value);
}
