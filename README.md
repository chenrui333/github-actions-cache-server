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

Direct downloads (`ENABLE_DIRECT_DOWNLOADS=true`) hand the runner a signed storage URL for merged entries instead of proxying the bytes.
They require token validation: the server refuses to start with `SKIP_TOKEN_VALIDATION=true`, because unverified callers could then obtain signed URLs for any repository's cache.

| Variable                                                 | Default | Effect                                                                                            |
| -------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------- |
| `DIRECT_DOWNLOAD_URL_TTL_SECONDS`                        | `600`   | Lifetime of a signed download URL, `1` to `3600`. Lower it to limit replay of a leaked URL.       |
| `ACTIONS_TOKEN_AUDIENCE`                                 | unset   | Comma-separated `aud` values a token must carry; unset skips the check, an empty list is refused. |
| `UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION` | `false` | Allows direct downloads with `SKIP_TOKEN_VALIDATION=true`. Local testing only.                    |

## Documentation

👉 <https://gha-cache-server.falcondev.io/getting-started> 👈
