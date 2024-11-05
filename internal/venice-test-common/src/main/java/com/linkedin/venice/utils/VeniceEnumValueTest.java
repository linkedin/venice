package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.testng.annotations.Test;


/**
 * Abstract class which makes it as easy as possible to generically test all the assumptions for enums which implement
 * the {@link VeniceEnumValue} interface. Subclasses only need to implement the constructor and the abstract function.
 *
 * @param <T> the enum class under test
 */
@Test
public abstract class VeniceEnumValueTest<T extends VeniceEnumValue> {
  private static final int INVALID_NEGATIVE_VALUE = -1;
  private static final String VALUE_OF_METHOD_NAME = "valueOf";
  private static final String VALUES_METHOD_NAME = "values";
  private static final String ASSERTION_ERROR_MESSAGE =
      "The value ID of enums should not be changed, as that is backwards incompatible.";
  private final Class<T> enumClass;

  protected VeniceEnumValueTest(Class<T> enumClass) {
    this.enumClass = enumClass;
  }

  protected abstract Map<Integer, T> expectedMapping();

  @Test
  public void test() {
    // Check that there is a valueOf function which respects the expected contract
    Method valueOfMethod = getPublicStaticFunction(this.enumClass, VALUE_OF_METHOD_NAME, int.class);

    assertTrue(Modifier.isStatic(valueOfMethod.getModifiers()), "The " + VALUE_OF_METHOD_NAME + " should be static!");
    assertTrue(Modifier.isPublic(valueOfMethod.getModifiers()), "The " + VALUE_OF_METHOD_NAME + " should be public!");

    // check if there is a TYPES field which is either a List or a Map
    Field typesField = getPrivateStaticField(this.enumClass, "TYPES");
    boolean isStrictCheck = true;
    if (List.class.isAssignableFrom(typesField.getType())) {
      isStrictCheck = true;
    } else if (Map.class.isAssignableFrom(typesField.getType())) {
      isStrictCheck = false;
    } else {
      fail("The TYPES field should be a List or a Map!");
    }

    Function<Integer, T> valueOfFunction = value -> {
      try {
        return (T) valueOfMethod.invoke(null, value);
      } catch (Exception e) {
        if (e.getClass() == InvocationTargetException.class && e.getCause() instanceof VeniceException) {
          // Those are expected for invalid values, so we bubble them up.
          throw (VeniceException) e.getCause();
        }
        fail("The " + VALUE_OF_METHOD_NAME + " threw an exception!", e);
        // N.B.: Although the return statement below is unreachable, since fail will throw, the compiler does not know
        // that.
        return null;
      }
    };

    Map<Integer, T> expectedMapping = expectedMapping();
    assertFalse(expectedMapping.isEmpty());

    // Check that all mappings are as expected
    int highestValue = INVALID_NEGATIVE_VALUE;
    for (Map.Entry<Integer, T> entry: expectedMapping.entrySet()) {
      assertEquals(valueOfFunction.apply(entry.getKey()), entry.getValue(), ASSERTION_ERROR_MESSAGE);
      assertEquals(entry.getValue().getValue(), entry.getKey().intValue(), ASSERTION_ERROR_MESSAGE);
      highestValue = Math.max(entry.getKey(), highestValue);
    }

    if (isStrictCheck) {
      // Check that out of bound IDs throw exceptions
      assertNotEquals(highestValue, INVALID_NEGATIVE_VALUE, "There are no values at all in the enum!");
      assertThrows(VeniceException.class, () -> valueOfFunction.apply(INVALID_NEGATIVE_VALUE));
      final int tooHighValue = highestValue + 1;
      assertThrows(VeniceException.class, () -> valueOfFunction.apply(tooHighValue));
    }

    // Check that no other enum values exist besides those that are expected
    Method valuesFunction = getPublicStaticFunction(this.enumClass, VALUES_METHOD_NAME, new Class[0]);
    try {
      T[] types = (T[]) valuesFunction.invoke(null, new Class[0]);
      for (T type: types) {
        assertTrue(
            expectedMapping.containsKey(type.getValue()),
            "Class " + this.enumClass.getSimpleName() + " contains an unexpected value: " + type.getValue());
      }
    } catch (Exception e) {
      fail("The " + VALUES_METHOD_NAME + " threw an exception!", e);
    }
  }

  private static Method getPublicStaticFunction(Class klass, String functionName, Class... params) {
    try {
      Method function = klass.getDeclaredMethod(functionName, params);
      assertTrue(
          Modifier.isStatic(function.getModifiers()),
          "Class " + klass.getSimpleName() + " should have a static " + functionName + " function!");
      assertTrue(
          Modifier.isPublic(function.getModifiers()),
          "Class " + klass.getSimpleName() + " should have a public " + functionName + " function!");
      return function;
    } catch (NoSuchMethodException e) {
      fail("Class " + klass.getSimpleName() + " should have a " + functionName + " method!", e);
      // N.B.: Although the return statement below is unreachable, since fail will throw, the compiler does not know
      // that.
      return null;
    }
  }

  private static Field getPrivateStaticField(Class klass, String fieldName) {
    try {
      Field field = klass.getDeclaredField(fieldName);
      assertTrue(
          Modifier.isStatic(field.getModifiers()),
          "Class " + klass.getSimpleName() + " should have a static " + fieldName + " field!");
      assertTrue(
          Modifier.isPrivate(field.getModifiers()),
          "Class " + klass.getSimpleName() + " should have a private " + fieldName + " field!");
      return field;
    } catch (NoSuchFieldException e) {
      fail("Class " + klass.getSimpleName() + " should have a " + fieldName + " field!", e);
      // N.B.: Although the return statement below is unreachable, since fail will throw, the compiler does not know
      // that.
      return null;
    }
  }
}
