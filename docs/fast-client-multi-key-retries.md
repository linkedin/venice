# Staging Fast Client metadata schema v5

This change adds the v5 schema resource with a default-empty `multiKeyLongTailRetryThresholdsInMs` string for a future
shared compute/batch-get retry policy. It does not activate the schema or change retry behavior.

`build.gradle` pins `MetadataResponseRecord` generation to v4 through `versionOverrides`. The generated record, server
writer-schema header and `SERVER_METADATA_RESPONSE` protocol version remain v4. Controllers continue registering schemas
only through v4; the presence of the v5 resource does not register it.

## Activation and rollout

1. Merge the schema-staging change first. Keep historical schemas unchanged.
2. Merge/release the activation and retry-policy change, which removes the v4 generation pin, advances the protocol
   version to 5 and initializes the new field. Deploy that release to **controllers/schema-registration components
   only** and verify schema ID 5 and its exact schema in every relevant metadata response system-schema store.
3. Only after registration is verified, deploy servers using the activation release, initially with empty policy. These
   servers emit v5 even when policy is empty. The endpoint has no request-version negotiation.
4. Roll compatible clients, then configure policies consistently across the intended server cluster. Retry tuning and
   request-cap changes are separate work.

Schema staging alone does not prepare registration for the activation release. Keep older schema IDs available and do
not couple the initial schema-aware controller rollout to a server rollout.
