package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreViewConfig;


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
}
