package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.Collections;
import java.util.Map;


public class ReadOnlyViewConfig implements ViewConfig {
  private final ViewConfig delegate;

  public ReadOnlyViewConfig(ViewConfig delegate) {
    this.delegate = delegate;
  }

  @Override
  public StoreViewConfig dataModel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ViewType getViewType() {
    return delegate.getViewType();
  }

  @Override
  public Map<String, String> getParams() {
    return Collections.unmodifiableMap(this.delegate.getParams());
  }
}
