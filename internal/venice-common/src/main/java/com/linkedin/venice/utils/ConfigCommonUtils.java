package com.linkedin.venice.utils;

public class ConfigCommonUtils {
  /**
   * Enum representing the config type.
   *
   * This enum provides a more flexible alternative to simple boolean configurations by
   * distinguishing between "explicitly disabled" and "unset (default)" states.
   *
   * ENABLED: Feature explicitly enabled.
   * DISABLED: Feature explicitly disabled.
   * NOT_SPECIFIED: Feature left unset, default rules apply.
   *
   * Put this in the common utils package so that it can be used in both router, server and controller.
   */
  public enum ActivationState {
    NOT_SPECIFIED, ENABLED, DISABLED
  }
}
