package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import static org.testng.Assert.*;

import static org.apache.avro.Schema.*;

import com.linkedin.venice.meta.Store;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * Test cases for Venice {@link Utils}
 */
public class UtilsTest {

  @Test
  public void testGetHelixNodeIdentifier() {
    int port = 1234;
    String identifier = Utils.getHelixNodeIdentifier(1234);
    assertEquals(identifier, Utils.getHostName() + "_" + port,
        "Identifier is not the valid format required by Helix.");
  }

  @Test
  public void testParseHostAndPortFromNodeIdentifier() {
    int port = 1234;
    String identifier = Utils.getHelixNodeIdentifier(1234);
    assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), Utils.getHostName());
    assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);

    identifier = "my_host_" + port;
    assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), "my_host");
    assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);

    identifier = "my_host_abc";
    assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), "my_host");
    try {
      assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);
      fail("Port should be numeric value");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testListEquals() {
    /** 1. Standard {@link java.util.ArrayList<Integer>} instances should compare properly. */

    List<Integer> javaUtilArrayList1 = new ArrayList<>(), javaUtilArrayList2 = new ArrayList<>();
    populateIntegerList(javaUtilArrayList1);
    populateIntegerList(javaUtilArrayList2);

    assertListEqualityBothWays(javaUtilArrayList1, javaUtilArrayList2,
        "We cannot compare java.util.ArrayList<Integer> by referential equality properly!");

    /**
     * 2. {@link org.apache.avro.generic.GenericData.Array} should compare properly.
     *
     * (This is the main reason for having the {@link Utils#listEquals(List, List)} function).
     */

    List<Integer> avroArray = new GenericData.Array<>(3, createArray(create(Type.INT)));
    populateIntegerList(avroArray);

    // Sanity check. This works:
    assertTrue(javaUtilArrayList1.equals(avroArray), "Java is broken!!!");

    // But this doesn't:
    assertFalse(avroArray.equals(javaUtilArrayList1), "Avro is not broken anymore!!!");

    /**
     * N.B.: The bad behavior demonstrated by the above assert is the reason why we are using
     * our own list equality implementation. If this assertion fails in the future (let's say,
     * following an upgrade of Avro), then that means we can get rid of our
     * {@link Utils#listEquals(List, List)} function.
     */

    // Code under test
    assertTrue(Utils.listEquals(javaUtilArrayList1, avroArray),
        "We cannot compare java.util.ArrayList<Integer> with GenericData.Array properly!");
    assertTrue(Utils.listEquals(avroArray, javaUtilArrayList1),
        "We cannot compare GenericData.Array with java.util.ArrayList<Integer> properly!");

    /** 3. Ensure that we verify content equality, not just referential equality */

    List<Store> javaUtilArrayList3 = new ArrayList<>(), javaUtilArrayList4 = new ArrayList<>();
    populateStoreList(javaUtilArrayList3);
    populateStoreList(javaUtilArrayList4);

    assertListEqualityBothWays(javaUtilArrayList3, javaUtilArrayList4,
        "We cannot compare java.util.ArrayList<Object> by content equality properly!");
  }
  
  private void populateIntegerList(List<Integer> list) {
    list.add(1);
    list.add(2);
    list.add(3);
  }

  private void populateStoreList(List<Store> list) {
    list.add(TestUtils.createTestStore("store1", "owner1", 123));
    list.add(TestUtils.createTestStore("store2", "owner1", 123));
    list.add(TestUtils.createTestStore("store3", "owner1", 123));
  }

  private <T> void assertListEqualityBothWays(List<T> list1, List<T> list2, String errorMessage) {
    // Sanity checks
    assertTrue(list1.equals(list2), "Java is broken!!!");
    assertTrue(list2.equals(list1), "Java is broken!!!");

    // Code under test
    assertTrue(Utils.listEquals(list2, list1), errorMessage);
    assertTrue(Utils.listEquals(list1, list2), errorMessage);
  }
}
