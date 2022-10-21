package com.linkedin.venice.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class FilterChain<INPUT_VALUE> implements Closeable {
  private List<AbstractVeniceFilter<INPUT_VALUE>> filterList;

  public FilterChain() {
    this.filterList = new LinkedList<>();
  }

  public FilterChain(AbstractVeniceFilter<INPUT_VALUE>... filters) {
    this.filterList = new LinkedList<>();
    filterList.addAll(Arrays.asList(filters));
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
    for (AbstractVeniceFilter<INPUT_VALUE> filter: filterList) {
      if (filter.apply(value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    for (AbstractVeniceFilter<INPUT_VALUE> filter: filterList) {
      filter.close();
    }
  }
}
