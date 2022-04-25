package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import java.security.cert.X509Certificate;
import java.util.Optional;


public interface LingeringStoreVersionChecker {
  /**
   * Check if a version has been lingering around
   * @param store
   * @param version
   * @return true if the provided version is has been lingering (so that it can be killed potentially)
   */
  boolean isStoreVersionLingering(
      Store store,
      Version version,
      Time time,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser);
}
