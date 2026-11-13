---
status: proposed
---

# Upload URLs carry a capability

Twirp calls are authorized by the Actions runtime token (`getCacheScope`), but the Upload URL returned in `signed_upload_url` is fetched by Azure Blob-style clients that send no token. Until now that URL held only the numeric Upload id, so it was the sole thing standing between any client that can reach the server and the Upload's Parts. We now issue a random 256-bit capability with each Upload, put it in the URL's query (`?sig=…`), and require it on every block, single-shot and blocklist request. A missing or wrong capability and an unknown Upload all get the same `403`, so the response reveals nothing about which Uploads exist.

The capability lives in the query because every client we support treats the signed URL as an Azure SAS URL: `@actions/cache` and `go-actions-cache` keep the query and append `comp`/`blockid`, and OpenDAL (sccache) requires a query and re-sends it as the SAS. Only its SHA-256 is stored (`uploads.capabilityHash`), compared in constant time, so neither the database nor its debug query log can reproduce a working URL. Request logs record paths without their query string, and upload errors are logged by the route rather than left to Nitro, whose unhandled-error log includes the request URL. Upload URLs requested with a method other than `PUT` get `405` instead of falling through to the Results Passthrough, which would forward the capability upstream.

We rejected an HMAC over the Upload id: it needs a secret shared by every worker and kept across restarts, which this server has no place for, while a stored random value needs nothing new. Uploads created before the migration have no capability, so their URLs stop accepting Parts; they are short-lived and `cleanup:uploads` removes them once idle.

## Authenticated completion, not key-only publication

The capability must fence the complete payload, not just individual writes.
A whole Put Blob carries the upload ID and capability and declares one complete
HTTP body. A Put Block List carries that same generation and the client's exact
ordered manifest after staging. Publish only successfully persisted immutable
revisions referenced by that authenticated boundary. Opaque IDs are not numeric
indexes; repeated IDs repeat bytes and omitted staged blocks are not appended.
Reject truncated bodies, unavailable references and concurrent unfinished writes.

Twirp Finalize has only key/version/size plus repository/ref authorization, not an
upload generation. It is therefore a read-only acknowledgment of an existing
completion receipt. An old or same-token finalizer cannot delete an empty
successor, publish its first part, or choose arbitrary current staged data.
This uses existing GitHub/BuildKit/OpenDAL transports without a new client nonce.
When token verification is enabled, new generations retain only the verified
creator JWT expiration as `publicationExpiresAt`, never the bearer. Whole PUT
and manifest publication recheck that deadline under the generation transaction,
including after asynchronous storage and immediately before committing visibility.
The same fixed runtime bearer authorizes Create and Finalize in the supported
clients; capability possession must not extend publication beyond its verified
deadline. Receipt retries cannot publish new data after expiry.

Expiry-unknown pre-upgrade generations cannot safely acquire an invented
deadline. They fail closed with verification enabled and require the explicitly
approved drain/maintenance boundary in the changelog. This is not transparent
rolling-upgrade compatibility or security-owner approval. Validation-disabled
development mode still has no trusted JWT deadline; it must not be used in
production.

Completion receipts retain the generation's capability hash and completion
identity under the Storage Location lifecycle. Retries may acknowledge the same
immutable completion; a different payload or manifest cannot rewrite it.
Stage attempts use distinct physical revisions, so a late/retried writer cannot
overwrite a published Part. Key/ref/repository fences and V1 passthrough remain.

Each unfinished revision has its own 60-second renewable attempt lease. Writers
renew every ten seconds under the generation lock. Recovery settles only expired
attempts as failed, never promotes their objects, and never clears another live
worker. A resumed expired writer cannot renew or settle its removed revision;
retransmission uses a new immutable name. This lease fences storage attempts, not
JWT authority or reservation retirement.

Only an explicitly requested, unambiguous NULL-capability reservation idle for
more than seven days may be retired; modern and active reservations do not
expire through this path. It removes metadata only, not payloads or other keys.
This does not change the global cleanup-disable decision. The new ordered-Part
migration is forward-only and requires a separately approved writer/backup/client
drain boundary, as documented in the candidate patch notes.
