import { type } from 'arkenv'

export const envStorageDriverSchema = type.or(
  {
    'STORAGE_DRIVER': type.unit('s3'),
    'STORAGE_S3_BUCKET': 'string',
    'STORAGE_S3_SOCKET_TIMEOUT_MS': 'number.integer >= 0 = 10000',
    'STORAGE_S3_MAX_SOCKETS': 'number.integer >= 1 = 50',
    'STORAGE_S3_CONTROL_MAX_SOCKETS': 'number.integer >= 1 = 16',
    'STORAGE_S3_CONTROL_QUEUE_TIMEOUT_MS': 'number.integer >= 0 = 5000',
    'AWS_REGION': "string = 'us-east-1'",
    'AWS_ENDPOINT_URL?': 'string.url',
    'AWS_ACCESS_KEY_ID?': 'string',
    'AWS_SECRET_ACCESS_KEY?': 'string',
  },
  {
    STORAGE_DRIVER: type.unit('filesystem'),
    STORAGE_FILESYSTEM_PATH: 'string',
  },
  {
    'STORAGE_DRIVER': type.unit('gcs'),
    'STORAGE_GCS_BUCKET': 'string',
    'STORAGE_GCS_SERVICE_ACCOUNT_KEY?': 'string',
    'STORAGE_GCS_ENDPOINT?': 'string.url',
  },
  {
    'STORAGE_DRIVER': type.unit('azblob'),
    'STORAGE_AZBLOB_CONTAINER': 'string',
    'STORAGE_AZBLOB_CONNECTION_STRING': 'string',
    'STORAGE_AZBLOB_ACCOUNT?': 'undefined',
    'STORAGE_AZBLOB_ENDPOINT?': 'undefined',
  },
  {
    'STORAGE_DRIVER': type.unit('azblob'),
    'STORAGE_AZBLOB_CONTAINER': 'string',
    'STORAGE_AZBLOB_ACCOUNT': 'string',
    'STORAGE_AZBLOB_ENDPOINT?': 'string.url',
    'STORAGE_AZBLOB_CONNECTION_STRING?': 'undefined',
  },
)
export const envDbDriverSchema = type.or(
  type.or(
    {
      'DB_DRIVER': type.unit('postgres'),
      'DB_POSTGRES_DATABASE': 'string',
      'DB_POSTGRES_HOST': 'string',
      'DB_POSTGRES_PORT': 'number.port',
      'DB_POSTGRES_USER': 'string',
      'DB_POSTGRES_PASSWORD': 'string',
      'DB_POSTGRES_URL?': 'undefined',
    },
    {
      'DB_DRIVER': type.unit('postgres'),
      'DB_POSTGRES_URL': 'string',
      'DB_POSTGRES_DATABASE?': 'undefined',
      'DB_POSTGRES_HOST?': 'undefined',
      'DB_POSTGRES_PORT?': 'undefined',
      'DB_POSTGRES_USER?': 'undefined',
      'DB_POSTGRES_PASSWORD?': 'undefined',
    },
  ),
  {
    DB_DRIVER: type.unit('mysql'),
    DB_MYSQL_DATABASE: 'string',
    DB_MYSQL_HOST: 'string',
    DB_MYSQL_PORT: 'number.port',
    DB_MYSQL_USER: 'string',
    DB_MYSQL_PASSWORD: 'string',
  },
  {
    DB_DRIVER: type.unit('sqlite'),
    DB_SQLITE_PATH: 'string',
  },
)

export const envBaseSchema = type({
  'API_BASE_URL': type('string.url').pipe((s) => s.replace(/\/+$/, '')),
  'DEFAULT_ACTIONS_RESULTS_URL':
    "string.url = 'https://results-receiver.actions.githubusercontent.com'",
  'ACTIONS_TOKEN_ISSUER': "string.url = 'https://token.actions.githubusercontent.com'",
  'ACTIONS_TOKEN_JWKS_URL?': 'string.url',
  'ACTIONS_TOKEN_AUDIENCE?': 'string',
  'CACHE_CLEANUP_OLDER_THAN_DAYS': 'number = 90',
  'CACHE_MAX_SIZE_BYTES?': 'number.integer > 0',
  'CACHE_FILESYSTEM_MAX_USAGE_PERCENT': 'number > 0 & number <= 100 = 90',
  'ORPHANED_STORAGE_GRACE_PERIOD_HOURS': 'number.integer >= 1 = 24',
  'DISABLE_CLEANUP_JOBS?': 'boolean',
  'DEBUG?': 'unknown',
  'ENABLE_DIRECT_DOWNLOADS': 'boolean = false',
  'DIRECT_DOWNLOAD_URL_TTL_SECONDS': '1 <= number.integer <= 3600 = 300',
  'UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION': 'boolean = false',
  'EAGER_MERGE': 'boolean = false',
  'MAX_CONCURRENT_MERGES': 'number.integer >= 1 = 16',
  'DOWNLOAD_IDLE_TIMEOUT_MS': 'number.integer >= 0 = 300000',
  'BENCHMARK': 'boolean = false',
  'SKIP_TOKEN_VALIDATION': 'boolean = false',
  'MANAGEMENT_API_KEY?': 'string',
})

export const envSchema = envBaseSchema.and(envStorageDriverSchema).and(envDbDriverSchema)
export type Env = typeof envSchema.infer

/**
 * A direct-download URL is a bearer credential for the stored object. Handing
 * one out to a caller whose token was never verified lets anyone who can reach
 * the server read any repository's cache straight from storage, so the two
 * settings are refused together unless explicitly overridden for local testing.
 */
export function assertDirectDownloadsRequireTokenValidation(
  env: Pick<
    Env,
    | 'ENABLE_DIRECT_DOWNLOADS'
    | 'SKIP_TOKEN_VALIDATION'
    | 'UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION'
  >,
) {
  if (
    env.ENABLE_DIRECT_DOWNLOADS &&
    env.SKIP_TOKEN_VALIDATION &&
    !env.UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION
  )
    throw new Error(
      'ENABLE_DIRECT_DOWNLOADS=true requires token validation, but SKIP_TOKEN_VALIDATION=true. ' +
        'Unverified callers would receive signed storage URLs for any cache entry. ' +
        'Unset SKIP_TOKEN_VALIDATION, or set UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION=true for local testing only.',
    )
}

/**
 * The `aud` values a token must carry. Unset means no audience check; set but
 * listing no audience (blank, or only commas) is refused, so a typo cannot
 * silently switch the check off.
 */
export function parseActionsTokenAudience(value: string | undefined) {
  if (value === undefined) return
  const audience = value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
  if (audience.length === 0)
    throw new Error(
      'ACTIONS_TOKEN_AUDIENCE is set but lists no audience. ' +
        'Set one or more comma-separated audiences, or unset it to skip the audience check.',
    )
  return audience
}
