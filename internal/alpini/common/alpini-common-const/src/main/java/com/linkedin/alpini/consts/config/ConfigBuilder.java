package com.linkedin.alpini.consts.config;

/**
 * Used for creation and verification of configs
 * Forked from com.linkedin.databus.core.util @ r293057
 * @author cbotev
 *
 * @param <C>       the type of configs to be created
 */
public interface ConfigBuilder<C> {
  C build() throws InvalidConfigException;
}
