package com.linkedin.alpini.router;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Jemiah Westerman <jwesterman@linkedin.com>
 *
 * @version $Revision$
 */
public class MockMessages {
  public static final Map<String, String> SIMPLE_MULTIPART1_HEADERS;
  public static final Map<String, String> SIMPLE_MULTIPART1_PART1_HEADERS;
  public static final Map<String, String> SIMPLE_MULTIPART1_PART2_HEADERS;

  static {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Type", "multipart/byteranges; \n    boundary=\"----=_Part_814_1677890900.1326232529858\"");
    headers.put("Content-Length", "4325");
    headers.put("Connection", "keep-alive");
    SIMPLE_MULTIPART1_HEADERS = Collections.unmodifiableMap(headers);

    headers = new HashMap<String, String>();
    headers.put("Date", "Tue, 10 Jan 2012 21:55:29 GMT");
    headers.put("Content-Type", "application/json");
    headers.put("X-ESPRESSO-Response-Type", "SINGLE_RESOURCE");
    headers.put("X-ESPRESSO-Schema-Location", "/schemata/document/BizProfile/BizCompany/1");
    headers.put("Content-Location", "/BizProfile/BizCompany/568234");
    headers.put("Last-Modified", "Tue, 10 Jan 2012 00:24:50 GMT");
    headers.put("ETag", "2641822883");
    headers.put("X-ESPRESSO-Content-Status", "200 OK");
    headers.put("X-ESPRESSO-Served-By", "esv4-app63.stg.linkedin.com:12918");
    headers.put("X-ESPRESSO-Partition", "BizProfile_51");
    headers.put("Content-Length", "17");
    SIMPLE_MULTIPART1_PART1_HEADERS = Collections.unmodifiableMap(headers);

    headers = new HashMap<String, String>();
    headers.put("Date", "Tue, 10 Jan 2012 21:55:29 GMT");
    headers.put("Content-Type", "application/json");
    headers.put("X-ESPRESSO-Response-Type", "SINGLE_RESOURCE");
    headers.put("X-ESPRESSO-Schema-Location", "/schemata/document/BizProfile/BizCompany/1");
    headers.put("Content-Location", "/BizProfile/BizCompany/568235");
    headers.put("Last-Modified", "Tue, 10 Jan 2012 00:24:50 GMT");
    headers.put("ETag", "2640290224");
    headers.put("X-ESPRESSO-Content-Status", "200 OK");
    headers.put("X-ESPRESSO-Served-By", "esv4-app65.stg.linkedin.com:12918");
    headers.put("X-ESPRESSO-Partition", "BizProfile_32");
    headers.put("Content-Length", "17");
    SIMPLE_MULTIPART1_PART2_HEADERS = Collections.unmodifiableMap(headers);
  }

  public static final String SIMPLE_MULTIPART1_PART1_BODY = "{\"rec1\" : \"val1\"}";
  public static final String SIMPLE_MULTIPART1_PART2_BODY = "{\"rec2\" : \"val2\"}";

  public static final String SIMPLE_MULTIPART1 =
      "------=_Part_814_1677890900.1326232529858\n" + "Date: Tue, 10 Jan 2012 21:55:29 GMT\n"
          + "Content-Type: application/json\n" + "X-ESPRESSO-Response-Type: SINGLE_RESOURCE\n"
          + "X-ESPRESSO-Schema-Location: /schemata/document/BizProfile/BizCompany/1\n"
          + "Content-Location: /BizProfile/BizCompany/568234\n" + "Last-Modified: Tue, 10 Jan 2012 00:24:50 GMT\n"
          + "ETag: 2641822883\n" + "X-ESPRESSO-Content-Status: 200 OK\n"
          + "X-ESPRESSO-Served-By: esv4-app63.stg.linkedin.com:12918\n" + "X-ESPRESSO-Partition: BizProfile_51\n"
          + "Content-Length: 17\n" + "\n" + "{\"rec1\" : \"val1\"}\n" + "------=_Part_814_1677890900.1326232529858\n"
          + "Date: Tue, 10 Jan 2012 21:55:29 GMT\n" + "Content-Type: application/json\n"
          + "X-ESPRESSO-Response-Type: SINGLE_RESOURCE\n"
          + "X-ESPRESSO-Schema-Location: /schemata/document/BizProfile/BizCompany/1\n"
          + "Content-Location: /BizProfile/BizCompany/568235\n" + "Last-Modified: Tue, 10 Jan 2012 00:24:50 GMT\n"
          + "ETag: 2640290224\n" + "X-ESPRESSO-Content-Status: 200 OK\n"
          + "X-ESPRESSO-Served-By: esv4-app65.stg.linkedin.com:12918\n" + "X-ESPRESSO-Partition: BizProfile_32\n"
          + "Content-Length: 17\n" + "\n" + "{\"rec2\" : \"val2\"}\n" + "------=_Part_814_1677890900.1326232529858--\n"
          + "";
}
