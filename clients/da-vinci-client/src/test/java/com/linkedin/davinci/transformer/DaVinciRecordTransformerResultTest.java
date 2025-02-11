package com.linkedin.davinci.transformer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.exceptions.VeniceException;
import org.testng.annotations.Test;


public class DaVinciRecordTransformerResultTest {
  public final static String testString = "testString";

  @Test
  public void testPassingInTransformedToSingleArgumentConstructor() {
    assertThrows(
        VeniceException.class,
        () -> new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED));
  }

  @Test
  public void testPassingInSkipToTwoArgumentConstructor() {
    assertThrows(
        VeniceException.class,
        () -> new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.SKIP, testString));
  }

  @Test
  public void testPassingInUnchangedToTwoArgumentConstructor() {
    assertThrows(
        VeniceException.class,
        () -> new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED, testString));
  }

  @Test
  public void testRetrieveResult() {
    DaVinciRecordTransformerResult<String> transformerResult =
        new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, testString);

    assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.TRANSFORMED);
    assertEquals(transformerResult.getValue(), testString);
  }
}
