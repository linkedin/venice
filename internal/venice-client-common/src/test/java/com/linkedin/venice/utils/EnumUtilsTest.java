package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class EnumUtilsTest {
  private enum ValidEnum implements VeniceEnumValue {
    A(0), B(1), C(2);

    private final int value;

    ValidEnum(int value) {
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }
  }

  private enum InvalidEnum implements VeniceEnumValue {
    INVALID(-1);

    private final int value;

    InvalidEnum(int value) {
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }
  }

  private enum DuplicateEnum implements VeniceEnumValue {
    A(0), B(1), C(1);

    private final int value;

    DuplicateEnum(int value) {
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }
  }

  private enum GapEnum implements VeniceEnumValue {
    A(0), B(2);

    private final int value;

    GapEnum(int value) {
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }
  }

  private enum ValidEnumSparse implements VeniceEnumValue {
    A(10), B(20), C(30);

    private final int value;

    ValidEnumSparse(int value) {
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }
  }

  @Test
  public void testGetEnumValuesList() {

    List<ValidEnum> values = EnumUtils.getEnumValuesList(ValidEnum.class);
    assertEquals(3, values.size());
    assertEquals(ValidEnum.A, values.get(0));
    assertEquals(ValidEnum.B, values.get(1));
    assertEquals(ValidEnum.C, values.get(2));

    // Test with negative value
    assertThrows(IllegalStateException.class, () -> {
      EnumUtils.getEnumValuesList(InvalidEnum.class);
    });

    // Test with duplicate values
    assertThrows(IllegalStateException.class, () -> {
      EnumUtils.getEnumValuesList(DuplicateEnum.class);
    });

    // Test with gaps
    assertThrows(IllegalStateException.class, () -> {
      EnumUtils.getEnumValuesList(GapEnum.class);
    });
  }

  @Test
  public void testGetEnumValuesSparseList() {
    Map<Integer, ValidEnumSparse> values = EnumUtils.getEnumValuesSparseList(ValidEnumSparse.class);
    assertEquals(3, values.size());
    assertEquals(ValidEnumSparse.A, values.get(10));
    assertEquals(ValidEnumSparse.B, values.get(20));
    assertEquals(ValidEnumSparse.C, values.get(30));

    // Test with negative value
    Map<Integer, InvalidEnum> invalidEnumValues = EnumUtils.getEnumValuesSparseList(InvalidEnum.class);
    assertEquals(1, invalidEnumValues.size());
    assertEquals(InvalidEnum.INVALID, invalidEnumValues.get(-1));

    // Test with duplicate values
    assertThrows(IllegalStateException.class, () -> {
      EnumUtils.getEnumValuesSparseList(DuplicateEnum.class);
    });

    // Test with gaps
    EnumUtils.getEnumValuesSparseList(GapEnum.class);
  }

  @Test
  public void testValueOf() {
    List<ValidEnum> valuesList = EnumUtils.getEnumValuesList(ValidEnum.class);
    Map<Integer, ValidEnum> valuesMap = EnumUtils.getEnumValuesSparseList(ValidEnum.class);

    assertEquals(ValidEnum.A, EnumUtils.valueOf(valuesList, 0, ValidEnum.class));
    assertEquals(ValidEnum.B, EnumUtils.valueOf(valuesList, 1, ValidEnum.class));
    assertEquals(ValidEnum.C, EnumUtils.valueOf(valuesList, 2, ValidEnum.class));

    assertEquals(ValidEnum.A, EnumUtils.valueOf(valuesMap, 0, ValidEnum.class));
    assertEquals(ValidEnum.B, EnumUtils.valueOf(valuesMap, 1, ValidEnum.class));
    assertEquals(ValidEnum.C, EnumUtils.valueOf(valuesMap, 2, ValidEnum.class));

    // Test invalid value
    assertThrows(VeniceException.class, () -> EnumUtils.valueOf(valuesList, 3, ValidEnum.class));
    assertThrows(VeniceException.class, () -> EnumUtils.valueOf(valuesMap, 3, ValidEnum.class));
  }
}
