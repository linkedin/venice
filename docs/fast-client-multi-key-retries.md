# Fast Client metadata schema v5 rollout

This schema-only release reserves the shared compute/batch-get retry-policy field with an empty default. Server
configuration and dynamic client retry selection are delivered in the follow-up feature release.

## Schema rollout and rollback

This change activates metadata response schema **v5**; generated writer/reader schema and `SERVER_METADATA_RESPONSE`
version must advance together. Historical schemas v1-v4 must remain unchanged. Split delivery into a schema-only
merge/release followed by the retry-policy feature merge/release.

The schema-only release contains the v5 schema, protocol-version bump, empty-field initialization and generated-record
fixture compatibility changes. It does not read server policy configuration or change client retry selection. However,
schema generation already selects v5, `MetadataResponse` serializes against that generated schema, and its writer-schema
header follows `SERVER_METADATA_RESPONSE`. **A server using the schema-only release already emits v5 with an empty
policy.** Empty policy is not a wire-version switch, and this endpoint has no request-version negotiation.

1. Merge and release the schema-only changes. Deploy the matching library to **controllers/schema-registration
   components only**, and verify schema ID 5 and its exact schema in every relevant metadata response system-schema
   store **before deploying any server that contains either release**. The initialization routine registers versions
   through the active protocol enum; merely adding the schema resource does not register it. Keep older schemas
   available. Do not couple the initial controller rollout to a server rollout.
2. After registration is verified, merge/release the feature changes and update the matching OSS library dependencies
   for servers and clients. Deploy server capability initially with empty policy. Schema-only servers may also be
   deployed after the registration gate, but do not provide the dynamic retry feature.
3. Roll compatible clients and then configure server policies consistently in the intended clusters. The current-default
   table preserves delays; any new tuning or store-cap increase is separate work. Validate representative supported old
   clients: current clients can fetch the writer schema and resolve old/new records, but not every historical client is
   necessarily supported.

Avro v4 readers ignore the added v5 field; v5 readers default it to empty when reading v4 writers. Both require the
advertised writer schema to be available. An unavailable schema 5 can prevent metadata refresh, so wire compatibility
does not remove the registration prerequisite.

Rollback by deploying an empty policy to every server (or using an explicit local override). Clients fall back on their
next successful refresh. A successful old-schema response also withdraws the policy; a transport failure deliberately
does not. Mixed old/new servers or inconsistent configuration can alternate the effective policy, so converge the server
cohort and monitor refresh failures during rollout.
