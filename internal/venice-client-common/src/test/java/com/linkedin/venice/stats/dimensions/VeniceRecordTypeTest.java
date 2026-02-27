package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceRecordTypeTest extends VeniceDimensionInterfaceTest<VeniceRecordType> {
  protected VeniceRecordTypeTest() {
    super(VeniceRecordType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_RECORD_TYPE;
  }

  @Override
  protected Map<VeniceRecordType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceRecordType, String>mapBuilder()
        .put(VeniceRecordType.DATA, "data")
        .put(VeniceRecordType.REPLICATION_METADATA, "replication_metadata")
        .build();
  }
}
