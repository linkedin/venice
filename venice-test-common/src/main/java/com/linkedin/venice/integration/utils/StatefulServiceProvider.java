package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.Utils;
import java.io.File;


/**
 * A wrapper for {@link ServiceProvider} which
 * also passes in a data directory for persisting the service's state.
 *
 * @param <Service> The type of {@link ProcessWrapper} returned.
 */
interface StatefulServiceProvider<Service extends ProcessWrapper> extends ServiceProvider<Service> {
  default Service get(String serviceName) throws Exception {
    File dir = Utils.getTempDataDirectory(serviceName);
    return get(serviceName, dir);
  }

  Service get(String serviceName, File dataDirectory) throws Exception;
}
