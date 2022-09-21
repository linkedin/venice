package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreViewConfig;


public class ViewConfigImpl implements ViewConfig {
  private final StoreViewConfig viewConfig;

  public ViewConfigImpl(StoreViewConfig viewConfig) {
    this.viewConfig = viewConfig;
  }

  @Override
  public StoreViewConfig dataModel() {
    return this.viewConfig;
  }

  @Override
  public ViewType getViewType() {
    return ViewType.getViewTypeFromInt(this.viewConfig.getViewType());
  }
}
