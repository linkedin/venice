package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class HttpResponseStatusCodeTest extends VeniceDimensionInterfaceTest<HttpResponseStatusCode> {
  protected HttpResponseStatusCodeTest() {
    super(HttpResponseStatusCode.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
  }

  @Override
  protected Map<HttpResponseStatusCode, String> expectedDimensionValueMapping() {
    return CollectionUtils.<HttpResponseStatusCode, String>mapBuilder()
        .put(HttpResponseStatusCode.CONTINUE, "100")
        .put(HttpResponseStatusCode.SWITCHING_PROTOCOLS, "101")
        .put(HttpResponseStatusCode.PROCESSING, "102")
        .put(HttpResponseStatusCode.OK, "200")
        .put(HttpResponseStatusCode.CREATED, "201")
        .put(HttpResponseStatusCode.ACCEPTED, "202")
        .put(HttpResponseStatusCode.NON_AUTHORITATIVE_INFORMATION, "203")
        .put(HttpResponseStatusCode.NO_CONTENT, "204")
        .put(HttpResponseStatusCode.RESET_CONTENT, "205")
        .put(HttpResponseStatusCode.PARTIAL_CONTENT, "206")
        .put(HttpResponseStatusCode.MULTI_STATUS, "207")
        .put(HttpResponseStatusCode.MULTIPLE_CHOICES, "300")
        .put(HttpResponseStatusCode.MOVED_PERMANENTLY, "301")
        .put(HttpResponseStatusCode.FOUND, "302")
        .put(HttpResponseStatusCode.SEE_OTHER, "303")
        .put(HttpResponseStatusCode.NOT_MODIFIED, "304")
        .put(HttpResponseStatusCode.USE_PROXY, "305")
        .put(HttpResponseStatusCode.TEMPORARY_REDIRECT, "307")
        .put(HttpResponseStatusCode.PERMANENT_REDIRECT, "308")
        .put(HttpResponseStatusCode.BAD_REQUEST, "400")
        .put(HttpResponseStatusCode.UNAUTHORIZED, "401")
        .put(HttpResponseStatusCode.PAYMENT_REQUIRED, "402")
        .put(HttpResponseStatusCode.FORBIDDEN, "403")
        .put(HttpResponseStatusCode.NOT_FOUND, "404")
        .put(HttpResponseStatusCode.METHOD_NOT_ALLOWED, "405")
        .put(HttpResponseStatusCode.NOT_ACCEPTABLE, "406")
        .put(HttpResponseStatusCode.PROXY_AUTHENTICATION_REQUIRED, "407")
        .put(HttpResponseStatusCode.REQUEST_TIMEOUT, "408")
        .put(HttpResponseStatusCode.CONFLICT, "409")
        .put(HttpResponseStatusCode.GONE, "410")
        .put(HttpResponseStatusCode.LENGTH_REQUIRED, "411")
        .put(HttpResponseStatusCode.PRECONDITION_FAILED, "412")
        .put(HttpResponseStatusCode.REQUEST_ENTITY_TOO_LARGE, "413")
        .put(HttpResponseStatusCode.REQUEST_URI_TOO_LONG, "414")
        .put(HttpResponseStatusCode.UNSUPPORTED_MEDIA_TYPE, "415")
        .put(HttpResponseStatusCode.REQUESTED_RANGE_NOT_SATISFIABLE, "416")
        .put(HttpResponseStatusCode.EXPECTATION_FAILED, "417")
        .put(HttpResponseStatusCode.MISDIRECTED_REQUEST, "421")
        .put(HttpResponseStatusCode.UNPROCESSABLE_ENTITY, "422")
        .put(HttpResponseStatusCode.LOCKED, "423")
        .put(HttpResponseStatusCode.FAILED_DEPENDENCY, "424")
        .put(HttpResponseStatusCode.UNORDERED_COLLECTION, "425")
        .put(HttpResponseStatusCode.UPGRADE_REQUIRED, "426")
        .put(HttpResponseStatusCode.PRECONDITION_REQUIRED, "428")
        .put(HttpResponseStatusCode.TOO_MANY_REQUESTS, "429")
        .put(HttpResponseStatusCode.REQUEST_HEADER_FIELDS_TOO_LARGE, "431")
        .put(HttpResponseStatusCode.INTERNAL_SERVER_ERROR, "500")
        .put(HttpResponseStatusCode.NOT_IMPLEMENTED, "501")
        .put(HttpResponseStatusCode.BAD_GATEWAY, "502")
        .put(HttpResponseStatusCode.SERVICE_UNAVAILABLE, "503")
        .put(HttpResponseStatusCode.GATEWAY_TIMEOUT, "504")
        .put(HttpResponseStatusCode.HTTP_VERSION_NOT_SUPPORTED, "505")
        .put(HttpResponseStatusCode.VARIANT_ALSO_NEGOTIATES, "506")
        .put(HttpResponseStatusCode.INSUFFICIENT_STORAGE, "507")
        .put(HttpResponseStatusCode.NOT_EXTENDED, "510")
        .put(HttpResponseStatusCode.NETWORK_AUTHENTICATION_REQUIRED, "511")
        .put(HttpResponseStatusCode.UNKNOWN, "0")
        .build();
  }
}
