package com.linkedin.venice.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTrieBasedPathResourceRegistry {
  @Test
  public void testExactMatch() {
    PathResourceRegistry<String> registry = new TrieBasedPathResourceRegistry<>();
    registry.register("/a/b/c/d", "d_resource");
    registry.register("/a/b/c", "c_resource");
    registry.register("/a1/b", "a1_b_resource");
    registry.register("/a/*/c/*", "a_c_resource");

    Assert.assertEquals(registry.find("/a/b/c/d"), "d_resource");
    Assert.assertEquals(registry.find("a/b/c"), "c_resource");
    Assert.assertEquals(registry.find("/a/aa/c/cc"), "a_c_resource");
    Assert.assertNull(registry.find("/a/c"));
    Assert.assertNull(registry.find("a/b/c/d/e"));
  }

  @Test
  public void testWildCardMatch() {
    PathResourceRegistry<String> registry = new TrieBasedPathResourceRegistry<>();
    registry.register("/a/b/c/d", "d_resource");
    registry.register("/a/b/c", "c_resource");
    registry.register("/a1/b", "a1_b_resource");
    registry.register("/a/*/c/*", "a_c_resource");

    Assert.assertEquals(registry.find("/a/b/c/e"), "a_c_resource");
    Assert.assertEquals(registry.find("/a/e/c/e"), "a_c_resource");
    Assert.assertNull(registry.find("/a/b/c/d/e"));
    Assert.assertNull(registry.find("/"));
  }

  @Test
  public void testDeletePath() {
    PathResourceRegistry<String> registry = new TrieBasedPathResourceRegistry<>();
    registry.register("/a/b/c/d", "d_resource");
    registry.register("/a/b/c", "c_resource");
    registry.register("/a/b/c/e", "e_resource");

    Assert.assertEquals(registry.find("a/b/c/e"), "e_resource");
    Assert.assertEquals(registry.find("/a/b/c/d"), "d_resource");
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.unregister("/a/b/c/d");
    Assert.assertEquals(registry.find("a/b/c/e"), "e_resource");
    Assert.assertNull(registry.find("/a/b/c/d"));
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.unregister("/a/b/c/e");
    Assert.assertNull(registry.find("a/b/c/e"));
    Assert.assertNull(registry.find("/a/b/c/d"));
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.register("/a", "a_resource");
    registry.unregister("/a/b/c");
    Assert.assertNull(registry.find("/a/b/c"));
    Assert.assertEquals(registry.find("/a"), "a_resource");
  }

  @Test
  public void testDeleteWildCardPath() {
    PathResourceRegistry<String> registry = new TrieBasedPathResourceRegistry<>();
    registry.register("/a/*/c/d", "d_resource");
    registry.register("/a/b/c", "c_resource");
    registry.register("/a/*/c/e", "e_resource");

    Assert.assertEquals(registry.find("a/b/c/e"), "e_resource");
    Assert.assertEquals(registry.find("/a/b/c/d"), "d_resource");
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.unregister("/a/*/c/d");
    Assert.assertEquals(registry.find("a/b/c/e"), "e_resource");
    Assert.assertNull(registry.find("/a/b/c/d"));
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.unregister("/a/*/c/e");
    Assert.assertNull(registry.find("a/b/c/e"));
    Assert.assertNull(registry.find("/a/b/c/d"));
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.register("/a", "a_resource");
    registry.unregister("/a/*/c");
    Assert.assertEquals(registry.find("/a/b/c"), "c_resource");
    registry.unregister("/a/b/c");
    Assert.assertNull(registry.find("/a/b/c"));
    Assert.assertEquals(registry.find("/a"), "a_resource");
  }
}
