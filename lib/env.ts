import type { envDbDriverSchema, envStorageDriverSchema } from './schemas'
import arkenv from 'arkenv'
import {
  assertDirectDownloadsRequireTokenValidation,
  envSchema,
  parseActionsTokenAudience,
} from './schemas'

export const env = arkenv(envSchema, {
  env: Object.assign(
    {
      STORAGE_DRIVER: 'filesystem',
      STORAGE_FILESYSTEM_PATH: '.data/storage/filesystem',
      DB_DRIVER: 'sqlite',
      DB_SQLITE_PATH: '.data/sqlite.db',
    } satisfies typeof envStorageDriverSchema.infer & typeof envDbDriverSchema.infer,
    process.env,
  ),
})

assertDirectDownloadsRequireTokenValidation(env)

// GitHub-issued runtime tokens carry a deployment-specific `aud`, so pinning it
// restricts the server to one installation's tokens. Parsed here so a bad value
// stops startup.
export const actionsTokenAudience = parseActionsTokenAudience(env.ACTIONS_TOKEN_AUDIENCE)
