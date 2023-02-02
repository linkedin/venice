package com.linkedin.venice.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class SharedObjectFactoryTest {
  private class TestSharedObject implements AutoCloseable {
    String identifier;
    boolean destructorInvoked;

    TestSharedObject(String identifier) {
      this.identifier = identifier;
      destructorInvoked = false;
    }

    @Override
    public void close() {
      destructorInvoked = true;
    }
  }

  @Test
  public void testSharedObject() {
    SharedObjectFactory<TestSharedObject> factory = new SharedObjectFactory<>();

    String id1 = "id1";
    // Create a new shared object
    TestSharedObject obj1 = factory.get(id1, () -> new TestSharedObject(id1), TestSharedObject::close);
    Assert.assertFalse(obj1.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 1);

    // Get the same shared object from the factory
    TestSharedObject obj2 = factory.get(id1, () -> new TestSharedObject(id1), TestSharedObject::close);
    Assert.assertSame(obj1, obj2);
    Assert.assertFalse(obj1.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 2);

    Assert.assertEquals(factory.size(), 1);

    String id2 = "id2";
    // Create a new shared object with a different identifier
    TestSharedObject obj3 = factory.get(id2, () -> new TestSharedObject(id1), TestSharedObject::close);
    Assert.assertFalse(obj3.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id2), 1);

    Assert.assertEquals(factory.size(), 2);

    // Release a shared object. The destructor will not be invoked since the object is shared
    Assert.assertFalse(factory.release(id1));
    Assert.assertFalse(obj2.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 1);
    Assert.assertEquals(factory.size(), 2);

    // Release the last reference of a shared object. The destructor will now be invoked
    Assert.assertTrue(factory.release(id1));
    Assert.assertTrue(obj2.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 0);
    Assert.assertEquals(factory.size(), 1);

    // Release the last reference of a shared object. The destructor will now be invoked
    Assert.assertTrue(factory.release(id2));
    Assert.assertTrue(obj2.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 0);
    Assert.assertEquals(factory.size(), 0);

    // Calling release on a non-managed object returns true
    Assert.assertTrue(factory.release(id1));
    String id3 = "id3";
    Assert.assertTrue(factory.release(id3));
  }
}
