package com.linkedin.venice.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * The FilterChain class takes a list of {@link AbstractVeniceFilter} to assemble a filter chain to manage the life cycles'
 * of filters and perform filtering based on the order of filters.
 * If a record has been filtered by a predecessor, then it won't be passed to latter part of the filter chain.
 */
public class FilterChain<INPUT_VALUE> implements Closeable {
  private List<AbstractVeniceFilter<INPUT_VALUE>> filterList;

  public FilterChain() {
    this.filterList = new ArrayList<>();
  }

  public FilterChain(AbstractVeniceFilter<INPUT_VALUE>... filters) {
    this.filterList = Arrays.asList(filters);
  }

  public boolean isEmpty() {
    return filterList.isEmpty();
  }

  public void add(AbstractVeniceFilter<INPUT_VALUE> filter) {
    this.filterList.add(filter);
  }

  /**
   * This function passes the value through the filter chain to determine
   * if the value can be filtered out or not.
   * @param value
   * @return true if the value should be filtered out by this filter or its successor, otherwise false.
   */
  public boolean apply(final INPUT_VALUE value) {
    if (!filterList.isEmpty()) {
      for (AbstractVeniceFilter<INPUT_VALUE> filter: filterList) {
        if (filter.apply(value)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    for (AbstractVeniceFilter<INPUT_VALUE> filter: filterList) {
      filter.close();
    }
    filterList = null;
  }
}
