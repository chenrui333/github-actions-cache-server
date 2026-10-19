---
status: proposed
---

# Upload URLs carry a capability

Twirp calls are authorized by the Actions runtime token (`getCacheScope`), but the Upload URL returned in `signed_upload_url` is fetched by Azure Blob-style clients that send no token. Until now that URL held only the numeric Upload id, so it was the sole thing standing between any client that can reach the server and the Upload's Parts. We now issue a random 256-bit capability with each Upload, put it in the URL's query (`?sig=…`), and require it on every block, single-shot and blocklist request. A missing or wrong capability and an unknown Upload all get the same `403`, so the response reveals nothing about which Uploads exist.

The capability lives in the query because every client we support treats the signed URL as an Azure SAS URL: `@actions/cache` and `go-actions-cache` keep the query and append `comp`/`blockid`, and OpenDAL (sccache) requires a query and re-sends it as the SAS. Only its SHA-256 is stored (`uploads.capabilityHash`), compared in constant time, so neither the database nor its debug query log can reproduce a working URL; upload errors are logged by the route rather than left to Nitro, whose unhandled-error log includes the request URL.

We rejected an HMAC over the Upload id: it needs a secret shared by every worker and kept across restarts, which this server has no place for, while a stored random value needs nothing new. Uploads created before the migration have no capability, so their URLs stop accepting Parts; they are short-lived and `cleanup:uploads` removes them once idle.
