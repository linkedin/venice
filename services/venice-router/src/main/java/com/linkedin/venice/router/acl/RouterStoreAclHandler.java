package com.linkedin.venice.router.acl;

import static com.linkedin.venice.router.api.RouterResourceType.TYPE_COMPUTE;
import static com.linkedin.venice.router.api.RouterResourceType.TYPE_INVALID;
import static com.linkedin.venice.router.api.RouterResourceType.TYPE_STORAGE;

import com.linkedin.venice.acl.AclCreationDeletionListener;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.AbstractStoreAclHandler;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.router.api.RouterResourceType;
import io.netty.channel.ChannelHandler;


/**
 * Store-level access control handler, which is being used by both Router and Server.
 */
@ChannelHandler.Sharable
public class RouterStoreAclHandler extends AbstractStoreAclHandler<RouterResourceType> {
  public RouterStoreAclHandler(
      IdentityParser identityParser,
      DynamicAccessController accessController,
      ReadOnlyStoreRepository metadataRepository) {
    super(identityParser, accessController, metadataRepository);
    metadataRepository.registerStoreDataChangedListener(new AclCreationDeletionListener(accessController));
  }

  @Override
  protected boolean needsAclValidation(RouterResourceType resourceType) {
    if (resourceType == TYPE_STORAGE || resourceType == TYPE_COMPUTE) {
      return true;
    }

    if (resourceType == TYPE_INVALID) {
      throw new VeniceUnsupportedOperationException(resourceType.name());
    }

    return false;
  }

  /**
   * Extract the store name from the incoming resource name.
   */
  @Override
  protected String extractStoreName(RouterResourceType resourceType, String[] requestParts) {
    // In Routers, all requests that go through ACL checks have the 2nd part as the store name
    return requestParts[2];
  }

  @Override
  protected RouterResourceType validateRequest(String[] requestParts) {
    int partsLength = requestParts.length;

    if (partsLength < 3) {
      // In routers, all requests have at least the request type and store name
      return null;
    } else { // throw exception to retain current behavior for invalid query actions
      String requestType = requestParts[1].toLowerCase();
      RouterResourceType resourceType = RouterResourceType.getTypeResourceType(requestType);
      if (resourceType == TYPE_INVALID) {
        return null;
      }
      return resourceType;
    }
  }
}
