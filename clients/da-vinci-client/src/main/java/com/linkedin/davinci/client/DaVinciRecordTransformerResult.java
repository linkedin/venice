package com.linkedin.davinci.client;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * This class encapsulates the result of {@link DaVinciRecordTransformer#transform}
 *
 * @param <O> the type of the output value
 */
public class DaVinciRecordTransformerResult<O> {
  public enum Result {
    SKIP, UNCHANGED, TRANSFORMED
  }

  private final Result result;
  private O value;

  /**
   * Use this constructor if result is {@link Result#SKIP} or {@link Result#UNCHANGED}
   */
  public DaVinciRecordTransformerResult(Result result) {
    if (result == Result.TRANSFORMED) {
      throw new VeniceException(
          "Invalid constructor usage:"
              + "TRANSFORMED result passed to single-argument constructor. Use the two-argument constructor instead");
    }

    this.result = result;
  }

  /**
   * Use this constructor if result is {@link Result#TRANSFORMED}
   */
  public DaVinciRecordTransformerResult(Result result, O value) {
    if (result != Result.TRANSFORMED) {
      throw new VeniceException(
          "Invalid constructor usage:" + "This two-argument constructor only accepts TRANSFORMED results");
    }
    this.result = result;
    this.value = value;
  }

  /**
   * @return {@link Result}
   */
  public Result getResult() {
    return result;
  }

  /**
   * @return the transformed record
   */
  public O getValue() {
    return value;
  }
}
