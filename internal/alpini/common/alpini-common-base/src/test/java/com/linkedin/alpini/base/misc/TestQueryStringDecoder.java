package com.linkedin.alpini.base.misc;

import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestQueryStringDecoder {
  @Test(groups = "unit")
  public void testBasic() {
    QueryStringDecoder qsd = new QueryStringDecoder("/hello/world");
    Assert.assertEquals(qsd.getPath(), "/hello/world");
    Assert.assertTrue(qsd.getParameters().isEmpty());
  }

  @Test(groups = "unit")
  public void testQuery() {
    QueryStringDecoder qsd = new QueryStringDecoder("/hello/world?name=foo");
    Assert.assertEquals(qsd.getPath(), "/hello/world");
    Assert.assertFalse(qsd.getParameters().isEmpty());
    Assert.assertEquals(qsd.getParameters().get("name"), Collections.singletonList("foo"));
    Assert.assertEquals(qsd.getParameters().size(), 1);

    qsd = new QueryStringDecoder("/hello/world?name=foo&bar=wibble");
    Assert.assertEquals(qsd.getPath(), "/hello/world");
    Assert.assertFalse(qsd.getParameters().isEmpty());
    Assert.assertEquals(qsd.getParameters().get("name"), Collections.singletonList("foo"));
    Assert.assertEquals(qsd.getParameters().get("bar"), Collections.singletonList("wibble"));
    Assert.assertEquals(qsd.getParameters().size(), 2);

    qsd = new QueryStringDecoder("/hello/world?name=foo&bar=wibble&name=test");
    Assert.assertEquals(qsd.getPath(), "/hello/world");
    Assert.assertFalse(qsd.getParameters().isEmpty());
    Assert.assertEquals(qsd.getParameters().get("name"), Arrays.asList("foo", "test"));
    Assert.assertEquals(qsd.getParameters().get("bar"), Collections.singletonList("wibble"));
    Assert.assertEquals(qsd.getParameters().size(), 2);
  }
}
