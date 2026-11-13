# Changelog

## 9.8.0-backpressure.4 — unpublished candidate

### Correctness and authorization

- Publish only the exact capability-authenticated generation at complete whole
  PUT or validated Azure Put Block List. Preserve canonical opaque block IDs,
  submitted order, repeated references and omitted staged blocks. New immutable
  physical revisions prevent concurrent or late writes from replacing selected
  bytes. Body framing, actual sizes and optional Content-MD5 are checked.
- Key-only Twirp Finalize is a read-only repository/ref-scoped acknowledgment of
  an immutable completion receipt, never a selector of pending successor bytes.
  Completed-key replacement remains supported; generation immutability does not
  mean first-writer-wins keys. Receipt retries cannot alter the published payload.
- With token verification enabled, store only the verified creator JWT expiration
  on new reservations. Recheck it under the publication transaction after body
  and storage I/O and before committing visibility. No bearer is persisted. The
  supported clients retain their runtime bearer across Create and Finalize:
  [Actions cache](https://github.com/actions/toolkit/tree/main/packages/cache),
  [BuildKit 0.34.2](https://github.com/moby/buildkit/blob/v0.34.2/vendor/github.com/tonistiigi/go-actions-cache/cache_v2.go)
  and [OpenDAL 0.55](https://github.com/apache/opendal/blob/v0.55.0/core/src/services/ghac/writer.rs).
  This bounds capability publication; it does not claim live-upgrade equivalence
  for historical unknown-expiry clients. Validation-disabled development mode has
  no verified deadline and remains unsuitable for production.
- Give each unfinished write attempt a separately renewable 60-second lease,
  renewed every ten seconds under the generation lock. Retransmission/completion
  and idle cleanup settle only expired attempts as failed, never infer success
  from objects or counters. A resumed expired owner cannot renew or settle its
  fenced revision. Other workers' live attempts continue to fence publication
  and cleanup. Payload streaming does not require buffering a whole cache.
- Requested-key metadata retirement applies only to unambiguous NULL-capability
  reservations whose creation and last progress are over seven days old, with
  settled attempts and no completed entry or shared location. Modern reservations
  are not reclaimed here. Seven days exceeds the documented five-day self-hosted
  job limit plus two days of margin, not a JWT-expiry assertion. Global cleanup
  configuration, V1 passthrough, secrets and issuer settings are unchanged.

### Forward-only migration and explicit compatibility gate

`$9_authenticated_upload_completion` adds immutable attempt metadata, completion
receipts, ordered Part names and nullable publication deadlines. Existing completed
entries retain their numeric layout and remain readable. No historical deadline
or opaque block identity is manufactured from row age, counters or object names.

**A transparent rolling upgrade is not supported or approved.** Before any rollout,
a Security OWNER and service owner must approve a writer-fenced client drain or
maintenance/restart boundary for all pre-upgrade active reservations. In particular:

- Existing capability-bearing reservations have unknown verified expiry. With
  verification enabled, their whole PUT and block-list publication fail closed
  rather than gaining indefinite authority. Fresh key-only Finalize cannot publish
  their raw or pending parts, and these reservations are not silently reclaimed.
- Pre-upgrade multipart blocks were stored under numeric names without an exact
  opaque-ID/revision manifest. A cross-upgrade block list cannot safely commit
  those old staged bytes. Clients must complete before the writer transition or
  restart under the separately approved maintenance plan. Automatic re-reservation
  of modern rows is deliberately not provided.
- Previously completed entries without receipts remain restorable; a delayed
  Finalize cannot manufacture a receipt. New-generation Finalize remains read-only.

No compatible immutable-seal/fresh-JWT transition for all historical requests is
claimed. This drain/availability contract requires explicit owner approval; the
candidate remains draft and is not deployment-ready. This is not permission to
expand capability lifetime, disable validation, clear live rows or repair a cache.

The migration is **forward-only**: older binaries cannot read ordered immutable
Parts and reject the new migration ledger. Do not downgrade the image or drop new
metadata. An approved rollout requires a recoverable pre-migration database backup,
preserved payload storage and one fenced writer. Recovery is a separately reviewed
forward repair or explicitly approved pre-migration database/storage restoration;
restoring an older database can orphan newer payloads. No backup or migration is
performed by preparing this change.

### Qualification and release gates

Require the real backend CI matrix, supported-client cold-write/warm-read byte
parity, expiry-during-write and crash/late-writer regressions, current-head model
review and Security OWNER/service-owner approval. Actual Claude review remains
unexecuted under the explicit authentication HOLD; other independent reviews do
not replace it. No tag/image publication, merge, runtime operation, deployment or
release qualification is authorized. Recheck immutable version availability before
any separately approved patch-tag/OCI-archive publication.
