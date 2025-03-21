package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class HttpResponseStatusEnumTest extends VeniceDimensionInterfaceTest<HttpResponseStatusEnum> {
  protected HttpResponseStatusEnumTest() {
    super(HttpResponseStatusEnum.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
  }

  @Override
  protected Map<HttpResponseStatusEnum, String> expectedDimensionValueMapping() {
    return CollectionUtils.<HttpResponseStatusEnum, String>mapBuilder()
        .put(HttpResponseStatusEnum.CONTINUE, "100")
        .put(HttpResponseStatusEnum.SWITCHING_PROTOCOLS, "101")
        .put(HttpResponseStatusEnum.PROCESSING, "102")
        .put(HttpResponseStatusEnum.OK, "200")
        .put(HttpResponseStatusEnum.CREATED, "201")
        .put(HttpResponseStatusEnum.ACCEPTED, "202")
        .put(HttpResponseStatusEnum.NON_AUTHORITATIVE_INFORMATION, "203")
        .put(HttpResponseStatusEnum.NO_CONTENT, "204")
        .put(HttpResponseStatusEnum.RESET_CONTENT, "205")
        .put(HttpResponseStatusEnum.PARTIAL_CONTENT, "206")
        .put(HttpResponseStatusEnum.MULTI_STATUS, "207")
        .put(HttpResponseStatusEnum.MULTIPLE_CHOICES, "300")
        .put(HttpResponseStatusEnum.MOVED_PERMANENTLY, "301")
        .put(HttpResponseStatusEnum.FOUND, "302")
        .put(HttpResponseStatusEnum.SEE_OTHER, "303")
        .put(HttpResponseStatusEnum.NOT_MODIFIED, "304")
        .put(HttpResponseStatusEnum.USE_PROXY, "305")
        .put(HttpResponseStatusEnum.TEMPORARY_REDIRECT, "307")
        .put(HttpResponseStatusEnum.PERMANENT_REDIRECT, "308")
        .put(HttpResponseStatusEnum.BAD_REQUEST, "400")
        .put(HttpResponseStatusEnum.UNAUTHORIZED, "401")
        .put(HttpResponseStatusEnum.PAYMENT_REQUIRED, "402")
        .put(HttpResponseStatusEnum.FORBIDDEN, "403")
        .put(HttpResponseStatusEnum.NOT_FOUND, "404")
        .put(HttpResponseStatusEnum.METHOD_NOT_ALLOWED, "405")
        .put(HttpResponseStatusEnum.NOT_ACCEPTABLE, "406")
        .put(HttpResponseStatusEnum.PROXY_AUTHENTICATION_REQUIRED, "407")
        .put(HttpResponseStatusEnum.REQUEST_TIMEOUT, "408")
        .put(HttpResponseStatusEnum.CONFLICT, "409")
        .put(HttpResponseStatusEnum.GONE, "410")
        .put(HttpResponseStatusEnum.LENGTH_REQUIRED, "411")
        .put(HttpResponseStatusEnum.PRECONDITION_FAILED, "412")
        .put(HttpResponseStatusEnum.REQUEST_ENTITY_TOO_LARGE, "413")
        .put(HttpResponseStatusEnum.REQUEST_URI_TOO_LONG, "414")
        .put(HttpResponseStatusEnum.UNSUPPORTED_MEDIA_TYPE, "415")
        .put(HttpResponseStatusEnum.REQUESTED_RANGE_NOT_SATISFIABLE, "416")
        .put(HttpResponseStatusEnum.EXPECTATION_FAILED, "417")
        .put(HttpResponseStatusEnum.MISDIRECTED_REQUEST, "421")
        .put(HttpResponseStatusEnum.UNPROCESSABLE_ENTITY, "422")
        .put(HttpResponseStatusEnum.LOCKED, "423")
        .put(HttpResponseStatusEnum.FAILED_DEPENDENCY, "424")
        .put(HttpResponseStatusEnum.UNORDERED_COLLECTION, "425")
        .put(HttpResponseStatusEnum.UPGRADE_REQUIRED, "426")
        .put(HttpResponseStatusEnum.PRECONDITION_REQUIRED, "428")
        .put(HttpResponseStatusEnum.TOO_MANY_REQUESTS, "429")
        .put(HttpResponseStatusEnum.REQUEST_HEADER_FIELDS_TOO_LARGE, "431")
        .put(HttpResponseStatusEnum.INTERNAL_SERVER_ERROR, "500")
        .put(HttpResponseStatusEnum.NOT_IMPLEMENTED, "501")
        .put(HttpResponseStatusEnum.BAD_GATEWAY, "502")
        .put(HttpResponseStatusEnum.SERVICE_UNAVAILABLE, "503")
        .put(HttpResponseStatusEnum.GATEWAY_TIMEOUT, "504")
        .put(HttpResponseStatusEnum.HTTP_VERSION_NOT_SUPPORTED, "505")
        .put(HttpResponseStatusEnum.VARIANT_ALSO_NEGOTIATES, "506")
        .put(HttpResponseStatusEnum.INSUFFICIENT_STORAGE, "507")
        .put(HttpResponseStatusEnum.NOT_EXTENDED, "510")
        .put(HttpResponseStatusEnum.NETWORK_AUTHENTICATION_REQUIRED, "511")
        .put(HttpResponseStatusEnum.UNKNOWN, "0")
        .build();
  }
}
