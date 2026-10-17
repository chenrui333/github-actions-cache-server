# 🚀 GitHub Actions Cache Server

This is a drop-in replacement for the official GitHub hosted cache server. It is compatible with the official `actions/cache` action, so there is no need to change your workflow files and it even works with packages that internally use `actions/cache`.

## Features

- 🔥 **Compatible with official `actions/cache` action**
- 📦 Supports multiple storage solutions and is easily extendable.
- 🔒 Secure and self-hosted, giving you full control over your cache data.
- 😎 Easy setup

```yaml
services:
  cache-server:
    image: ghcr.io/falcondev-oss/github-actions-cache-server
    ports:
      - '3000:3000'
    environment:
      API_BASE_URL: http://localhost:3000
      STORAGE_DRIVER: filesystem
      STORAGE_FILESYSTEM_PATH: /data/cache
      DB_DRIVER: sqlite
      DB_SQLITE_PATH: /data/cache-server.db
    volumes:
      - cache-data:/data

volumes:
  cache-data:
```

Multipart downloads and background merges use stream backpressure and normal V8 garbage collection.
The server does not require Node's `--expose-gc` flag or force collection between parts.
Aggregate memory usage still depends on workload concurrency and configured storage buffers.

Resource limits (all optional):

| Variable                              | Default  | Effect                                                                                               |
| ------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `STORAGE_S3_MAX_SOCKETS`              | `50`     | Sockets per S3 data pool. Reads (GetObject) and writes (uploads, Merges) each get a pool this size.  |
| `STORAGE_S3_CONTROL_MAX_SOCKETS`      | `16`     | Sockets for HEAD, LIST and DELETE requests (cache lookups, cleanup), kept apart from bulk transfers. |
| `STORAGE_S3_CONTROL_QUEUE_TIMEOUT_MS` | `5000`   | How long a control request waits for a socket and connection before failing; `0` waits forever.      |
| `MAX_CONCURRENT_MERGES`               | `16`     | Merges one process runs at once. Beyond it, restores stream the parts and a later download merges.   |
| `DOWNLOAD_IDLE_TIMEOUT_MS`            | `300000` | Closes a download whose client has not read a byte for this long; `0` disables.                      |

## Documentation

👉 <https://gha-cache-server.falcondev.io/getting-started> 👈
