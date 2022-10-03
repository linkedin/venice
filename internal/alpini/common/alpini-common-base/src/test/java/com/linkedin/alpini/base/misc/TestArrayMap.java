package com.linkedin.alpini.base.misc;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestArrayMap {
  public void testBasic() {
    Map<String, String> map = new ArrayMap<>();

    Assert.assertTrue(map.isEmpty());
    Assert.assertNull(map.put("first", "value1"));
    Assert.assertFalse(map.isEmpty());
    Assert.assertEquals(map.get("first"), "value1");
    Assert.assertNull(map.get("second"));
    Assert.assertNull(map.put("second", "value2"));
    Assert.assertEquals(map.get("first"), "value1");
    Assert.assertEquals(map.get("second"), "value2");
    Assert.assertNull(map.put(null, "value3"));
    Assert.assertEquals(map.put("first", "value4"), "value1");
    Assert.assertEquals(map.get(null), "value3");
    Assert.assertEquals(map.get("first"), "value4");
    Map<String, String> map2 = new ArrayMap<>(map);
    Assert.assertTrue(map2.equals(map));
    Assert.assertEquals(map.remove("second"), "value2");
    Assert.assertNull(map.get("second"));
    Assert.assertFalse(map2.equals(map));
    map.clear();
    Assert.assertTrue(map.isEmpty());
    Assert.assertFalse(map2.isEmpty());
    Assert.assertEquals(map2.put(null, "value5"), "value3");
    Assert.assertEquals(map2.get("second"), "value2");
    Assert.assertEquals(map2.get(null), "value5");
  }

}
