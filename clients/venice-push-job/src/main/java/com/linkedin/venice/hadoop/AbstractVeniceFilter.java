package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.io.IOException;


/**
 * An abstraction to filter data using Chain of Responsibility pattern.
 * @param <INPUT_VALUE>
 */
public abstract class AbstractVeniceFilter<INPUT_VALUE> implements Closeable {
  protected final VeniceProperties props;

  public AbstractVeniceFilter(final VeniceProperties props) {
    this.props = props;
  }

  /**
   * This function implements how to parse the value and determine if filtering is needed.
   * @param value
   * @return true if the value should be filtered out, otherwise false.
   */
  public abstract boolean apply(final INPUT_VALUE value);

  /**
   * Close any resources that this filter holds.
   * @throws IOException
   */
  public abstract void close();
}
