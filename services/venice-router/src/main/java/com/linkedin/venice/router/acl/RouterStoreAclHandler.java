package com.linkedin.venice.router.acl;

import static com.linkedin.venice.router.api.RouterResourceType.TYPE_INVALID;

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
    switch (resourceType) {
      case TYPE_LEADER_CONTROLLER:
      case TYPE_LEADER_CONTROLLER_LEGACY:
      case TYPE_KEY_SCHEMA:
      case TYPE_VALUE_SCHEMA:
      case TYPE_LATEST_VALUE_SCHEMA:
      case TYPE_GET_UPDATE_SCHEMA:
      case TYPE_ALL_VALUE_SCHEMA_IDS:
      case TYPE_CLUSTER_DISCOVERY:
      case TYPE_STREAM_HYBRID_STORE_QUOTA:
      case TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA:
      case TYPE_STORE_STATE:
      case TYPE_PUSH_STATUS:
      case TYPE_ADMIN: // Access control for Admin operations are handled in AdminOperationsHandler
      case TYPE_CURRENT_VERSION:
      case TYPE_RESOURCE_STATE:
      case TYPE_BLOB_DISCOVERY:
      case TYPE_REQUEST_TOPIC:
        return false;
      case TYPE_STORAGE:
      case TYPE_COMPUTE:
        return true;
      case TYPE_INVALID:
      default:
        throw new VeniceUnsupportedOperationException(resourceType.name());
    }
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
