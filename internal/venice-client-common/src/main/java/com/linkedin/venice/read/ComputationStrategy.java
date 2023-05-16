package com.linkedin.venice.read;

import com.linkedin.venice.exceptions.VeniceException;


public enum ComputationStrategy {
  CLIENT_SIDE_COMPUTATION, SERVER_SIDE_COMPUTATION;

  public static ComputationStrategy valueOf(int ordinal) {
    try {
      return values()[ordinal];
    } catch (IndexOutOfBoundsException e) {
      throw new VeniceException("Invalid computation strategy: " + ordinal);
    }
  }
}
