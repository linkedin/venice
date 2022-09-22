package com.linkedin.alpini.consts.config;

/**
 * Forked from com.linkedin.databus.core.util @ r293057
 * @author sdas
 *
 */
public class InvalidConfigException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidConfigException(String msg) {
    super(msg);
  }

  public InvalidConfigException(Throwable cause) {
    super(cause);
  }

  public InvalidConfigException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
