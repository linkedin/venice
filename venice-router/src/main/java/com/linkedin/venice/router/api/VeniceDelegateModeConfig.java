package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.lnkd.RouterConfig;
import com.linkedin.venice.router.VeniceRouterConfig;


public class VeniceDelegateModeConfig {
  private boolean stickyRoutingEnabledForSingleGet;
  private boolean stickyRoutingEnabledForMultiGet;
  private boolean greedyMultiGetScatter;

  public VeniceDelegateModeConfig(){
    stickyRoutingEnabledForSingleGet = false;
    stickyRoutingEnabledForMultiGet = false;
    greedyMultiGetScatter = false;
  }

  public VeniceDelegateModeConfig(VeniceRouterConfig config){
    stickyRoutingEnabledForSingleGet = config.isStickyRoutingEnabledForSingleGet();
    stickyRoutingEnabledForMultiGet = config.isStickyRoutingEnabledForMultiGet();
    greedyMultiGetScatter = config.isGreedyMultiGet();
  }

  public VeniceDelegateModeConfig withStickyRoutingEnabledForSingleGet(boolean setting){
    this.stickyRoutingEnabledForSingleGet = setting;
    return this;
  }

  public VeniceDelegateModeConfig withStickyRoutingEnabledForMultiGet(boolean setting){
    this.stickyRoutingEnabledForMultiGet = setting;
    return this;
  }

  public VeniceDelegateModeConfig withGreedyMultiGetScatter(boolean setting){
    this.greedyMultiGetScatter = setting;
    return this;
  }

  public boolean isStickyRoutingEnabledForSingleGet() {
    return stickyRoutingEnabledForSingleGet;
  }

  public boolean isStickyRoutingEnabledForMultiGet() {
    return stickyRoutingEnabledForMultiGet;
  }

  public boolean isGreedyMultiGetScatter() {
    return greedyMultiGetScatter;
  }
}
