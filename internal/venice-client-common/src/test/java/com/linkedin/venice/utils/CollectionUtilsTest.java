package com.linkedin.venice.utils;

import static org.apache.avro.Schema.Type;
import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.Test;


public class CollectionUtilsTest {
  @Test
  public void testListEquals() {
    /** 1. Standard {@link java.util.ArrayList<Integer>} instances should compare properly. */

    List<Integer> javaUtilArrayList1 = new ArrayList<>(), javaUtilArrayList2 = new ArrayList<>();
    populateIntegerList(javaUtilArrayList1);
    populateIntegerList(javaUtilArrayList2);

    assertListEqualityBothWays(
        javaUtilArrayList1,
        javaUtilArrayList2,
        "We cannot compare java.util.ArrayList<Integer> by referential equality properly!");

    /**
     * 2. {@link org.apache.avro.generic.GenericData.Array} should compare properly.
     *
     * (This is the main reason for having the {@link CollectionUtils#listEquals(List, List)} function).
     */

    List<Integer> avroArray = new GenericData.Array<>(3, createArray(create(Type.INT)));
    populateIntegerList(avroArray);

    // Sanity check. This works:
    assertTrue(javaUtilArrayList1.equals(avroArray), "Java is broken!!!");

    // But this doesn't (in Avro 1.4 only):
    assertTrue(avroArray.equals(javaUtilArrayList1), "Avro is broken again somehow!!!");

    /**
     * N.B.: The bad behavior demonstrated by the above assert is the reason why we are using
     * our own list equality implementation. If this assertion fails in the future (let's say,
     * following an upgrade of Avro), then that means we can get rid of our
     * {@link CollectionUtils#listEquals(List, List)} function.
     *
     * Updates: The Avro version is updated to 1.7.7, so the above assert is changed; however,
     * in order to be compatible with clients who might still use Avro 1.4.1, we decided not
     * to remove {@link CollectionUtils#listEquals(List, List)} function yet.
     * More context: GenericData.Array.equals(Object o) in avro 1.4.1 checks whether 'o' is
     * an instance of GenericData.Array, while GenericData.Array.equals(Object o) in avro
     * 1.7.7 checks whether 'o' is an instance of List.
     */

    // Code under test
    assertTrue(
        CollectionUtils.listEquals(javaUtilArrayList1, avroArray),
        "We cannot compare java.util.ArrayList<Integer> with GenericData.Array properly!");
    assertTrue(
        CollectionUtils.listEquals(avroArray, javaUtilArrayList1),
        "We cannot compare GenericData.Array with java.util.ArrayList<Integer> properly!");

    /** 3. Ensure that we verify content equality, not just referential equality */

    List<TestContentObject> javaUtilArrayList3 = new ArrayList<>(), javaUtilArrayList4 = new ArrayList<>();
    populateTestObjectList(javaUtilArrayList3);
    populateTestObjectList(javaUtilArrayList4);

    assertListEqualityBothWays(
        javaUtilArrayList3,
        javaUtilArrayList4,
        "We cannot compare java.util.ArrayList<Object> by content equality properly!");
  }

  private void populateIntegerList(List<Integer> list) {
    list.add(1);
    list.add(2);
    list.add(3);
  }

  private void populateTestObjectList(List<TestContentObject> list) {
    list.add(new TestContentObject(1, true, "hello"));
    list.add(new TestContentObject(2, true, "hello"));
    list.add(new TestContentObject(3, true, "hello"));
  }

  private <T> void assertListEqualityBothWays(List<T> list1, List<T> list2, String errorMessage) {
    // Sanity checks
    assertTrue(list1.equals(list2), "Java is broken!!!");
    assertTrue(list2.equals(list1), "Java is broken!!!");

    // Code under test
    assertTrue(CollectionUtils.listEquals(list2, list1), errorMessage);
    assertTrue(CollectionUtils.listEquals(list1, list2), errorMessage);
  }

  class TestContentObject {
    Integer intVal;
    Boolean booleanVal;
    String stringVal;

    public TestContentObject(int intVal, boolean booleanVal, String stringVal) {
      this.intVal = intVal;
      this.booleanVal = booleanVal;
      this.stringVal = stringVal;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestContentObject testContentObject = (TestContentObject) o;
      if (!intVal.equals(testContentObject.intVal) || !booleanVal.equals(testContentObject.booleanVal)
          || !stringVal.equals(testContentObject.stringVal)) {
        return false;
      }
      return true;
    }
  }
}
