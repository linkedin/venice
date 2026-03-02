package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.CollectionUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class VeniceResponseStatusCategoryTest extends VeniceDimensionInterfaceTest<VeniceResponseStatusCategory> {
  protected VeniceResponseStatusCategoryTest() {
    super(VeniceResponseStatusCategory.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
  }

  @Override
  protected Map<VeniceResponseStatusCategory, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceResponseStatusCategory, String>mapBuilder()
        .put(VeniceResponseStatusCategory.SUCCESS, "success")
        .put(VeniceResponseStatusCategory.FAIL, "fail")
        .build();
  }

  @Test
  public void testGetVeniceResponseStatusCategory() throws IllegalAccessException {
    Set<HttpResponseStatus> successStatuses =
        new HashSet<>(Arrays.asList(HttpResponseStatus.OK, HttpResponseStatus.NOT_FOUND));

    for (Field field: HttpResponseStatus.class.getDeclaredFields()) {
      int mod = field.getModifiers();
      if (Modifier.isPublic(mod) && Modifier.isStatic(mod) && Modifier.isFinal(mod)
          && field.getType() == HttpResponseStatus.class) {
        HttpResponseStatus status = (HttpResponseStatus) field.get(null);
        VeniceResponseStatusCategory expected =
            successStatuses.contains(status) ? VeniceResponseStatusCategory.SUCCESS : VeniceResponseStatusCategory.FAIL;
        assertEquals(
            VeniceResponseStatusCategory.getVeniceResponseStatusCategory(status),
            expected,
            "Unexpected category for " + status);
      }
    }
  }
}
