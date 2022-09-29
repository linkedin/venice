package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;


/**
 * An abstraction to filter data using Chain of Responsibility pattern.
 * @param <INPUT_VALUE>
 */
public abstract class AbstractVeniceFilter<INPUT_VALUE> {
  protected final VeniceProperties props;

  public AbstractVeniceFilter(final VeniceProperties props) {
    this.props = props;
  }

  private AbstractVeniceFilter<INPUT_VALUE> next;

  /**
   * This function implements how to parse the value and determine if filtering is needed.
   * @param value
   * @return true if the value should be filtered out, otherwise false.
   */
  protected abstract boolean apply(final INPUT_VALUE value);

  /**
   * This function passes the value through the filter chain to determine
   * if the value can be filtered out or not.
   * @param value
   * @return true if the value should be filtered out by this filter or its successor, otherwise false.
   */
  public boolean applyRecursively(final INPUT_VALUE value) {
    if (apply((value))) {
      return true;
    } else {
      return next != null && next.applyRecursively(value);
    }
  }

  /**
   * Set the next filter for this chain.
   * @param filter, a chainable filter to handle <INPUT_VALUE>.
   */
  public void setNext(final AbstractVeniceFilter<INPUT_VALUE> filter) {
    this.next = filter;
  }

  /**
   * @return the next filter in this chain.
   */
  public AbstractVeniceFilter<INPUT_VALUE> next() {
    return this.next;
  }

  /**
   * @return true if the chain has more filters.
   */
  public boolean hasNext() {
    return this.next != null;
  }
}
