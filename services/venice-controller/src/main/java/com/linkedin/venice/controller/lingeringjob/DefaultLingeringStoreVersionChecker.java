package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


public class DefaultLingeringStoreVersionChecker implements LingeringStoreVersionChecker {
  public DefaultLingeringStoreVersionChecker() {
  }

  @Override
  public boolean isStoreVersionLingering(
      Store store,
      Version version,
      Time time,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser) {
    final long bootstrapDeadlineMs =
        version.getCreatedTime() + TimeUnit.HOURS.toMillis(store.getBootstrapToOnlineTimeoutInHours());
    return time.getMilliseconds() > bootstrapDeadlineMs;
  }
}
