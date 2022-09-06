package com.linkedin.venice.controllerapi.routes;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public interface V1Route {
  /**
   * Path formatted with colons prefixing each path parameter for use in defining the path in SparkJava
   * ex /user/:id
   * @return
   */
  String getRawPath();

  /**
   * The actual format string with "%s" in place of parameters that will be substituted
   * @return
   */
  String getPathFormat();

  /**
   * @return an array of key strings required for path resolution
   * @see #getPathWithParameters(Map)
   */
  String[] getPathParams();

  /**
   * Generates a path formatted with colons prefixing each path parameter for use in defining the path in SparkJava
   * ex /user/:id
   *
   * @param pathFormat Format string for the path to this resource
   * @param pathParams list of parameter names to use in completing the path
   * @return
   */
  static String rawPath(String pathFormat, String[] pathParams) {
    // Add colon prefix to each param name, "store_name" to ":store_name"
    String[] colonParams = Arrays.asList(pathParams).stream().map(p -> ":" + p).toArray(String[]::new);
    return String.format(pathFormat, (Object[]) colonParams);
  }

  /**
   * Similar to {@link #getPathWithParameters(Map)} but takes an ordered list of parameters for substitution.  It is
   * recommended to use that method and specify parameters by name.
   *
   * @see #getPathWithParameters(Map)
   * @param params
   * @return
   */
  default String getPathWithParameters(String[] params) {
    if (params.length != getPathParams().length) {
      throw new VeniceException(
          "Specified parameters: " + Arrays.toString(params) + " doesn't match list of required params: "
              + Arrays.toString(getPathParams()));
    }
    return String.format(getPathFormat(), (Object[]) params);
  }

  /**
   * Generate path with specified parameters substituted into the path.  This is the preferred way to generate
   * the path to a resource.
   *
   * Example:
   *
   * A route may define the path /user/:id.  To generate a path for id "123abc", pass into this method a Map that has
   * key: "id" and value: "123abc".  This will return the path /user/123abc
   *
   * The {@link #getPathParams()} method returns an array of key strings required.  {@link #getPathParamsList()} returns keys as a list.
   *
   * @param params
   * @return
   */
  default String getPathWithParameters(Map<String, String> params) {
    String[] pathParams = getPathParams();
    String[] paramsToUse = new String[pathParams.length];
    for (int i = 0; i < pathParams.length; i++) {
      if (!params.containsKey(pathParams[i])) {
        throw new VeniceException("Parameter map must contain parameter: " + pathParams[i]);
      }
      paramsToUse[i] = params.get(pathParams[i]);
    }
    return getPathWithParameters(paramsToUse);
  }

  default List<String> getPathParamsList() {
    return Arrays.asList(getPathParams());
  }
}
