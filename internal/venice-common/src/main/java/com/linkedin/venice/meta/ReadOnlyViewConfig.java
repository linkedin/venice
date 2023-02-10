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
  public String getViewClassName() {
    return delegate.getViewClassName();
  }

  @Override
  public Map<String, String> getViewParameters() {
    return Collections.unmodifiableMap(this.delegate.getViewParameters());
  }

  @Override
  public void setViewClassName(String className) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setViewParameters(Map<String, String> params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ViewConfig clone() {
    return new ReadOnlyViewConfig(delegate);
  }
}
