/**
 * API client.
 *
 * WHY THIS IS MORE THAN A fetch() WRAPPER
 *   The previous version threw `new Error("API error: " + res.status)` and
 *   discarded the response body. FastAPI puts the actual reason in `detail`, so
 *   every validation failure arrived as an opaque number and callers uniformly
 *   did `catch { setRows([]) }` — rendering an empty page that looked like a
 *   design decision rather than a fault.
 *
 *   That is exactly how the Missing Tracks page stayed broken: it asked for
 *   `per_page=500` against an endpoint capped at 200, got a 422 explaining
 *   precisely that, and showed a clean empty table instead.
 *
 *   So: keep the status, keep the detail, and make failures inspectable.
 */

const API_BASE = '/api'

/** An HTTP-level failure, carrying enough context to actually diagnose it. */
export class ApiError extends Error {
  readonly status: number
  readonly detail?: string
  readonly path: string

  constructor(status: number, path: string, detail?: string) {
    super(detail ? `${status} ${path}: ${detail}` : `${status} ${path}`)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
    this.path = path
  }

  /** 4xx means we sent something wrong; retrying sends the same wrong thing. */
  get isClientError(): boolean {
    return this.status >= 400 && this.status < 500
  }
}

/** Pull FastAPI's `detail` out of an error body, whatever shape it arrived in. */
async function extractDetail(res: Response): Promise<string | undefined> {
  try {
    const body = await res.json()
    const d = body?.detail
    if (typeof d === 'string') return d
    // 422 from pydantic is a list of {loc, msg, type}.
    if (Array.isArray(d)) {
      return d
        .map((e) => {
          const loc = Array.isArray(e?.loc) ? e.loc.filter((p: unknown) => p !== 'query').join('.') : ''
          return loc ? `${loc}: ${e?.msg}` : e?.msg
        })
        .filter(Boolean)
        .join('; ')
    }
    if (d != null) return JSON.stringify(d)
  } catch {
    /* non-JSON body; fall through */
  }
  return undefined
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export interface ApiOptions extends RequestInit {
  /** Retries for transport faults and 5xx only. Never applied to 4xx. */
  retries?: number
}

export async function apiFetch<T>(path: string, options?: ApiOptions): Promise<T> {
  const { retries = 2, ...init } = options ?? {}
  let lastError: unknown

  for (let attempt = 0; attempt <= retries; attempt++) {
    let res: Response
    try {
      res = await fetch(`${API_BASE}${path}`, {
        ...init,
        headers: { 'Content-Type': 'application/json', ...init.headers },
      })
    } catch (err) {
      // Transport-level failure (offline, DNS, connection reset).
      lastError = err
      if (attempt < retries) {
        await sleep(250 * 2 ** attempt)
        continue
      }
      throw err
    }

    if (res.ok) return res.json() as Promise<T>

    const error = new ApiError(res.status, path, await extractDetail(res))

    // A 4xx is our fault and is deterministic — surface it immediately.
    if (error.isClientError) throw error

    lastError = error
    if (attempt < retries) {
      await sleep(250 * 2 ** attempt)
      continue
    }
    throw error
  }

  throw lastError
}

export async function apiUpload<T>(path: string, formData: FormData): Promise<T> {
  // No Content-Type header: the browser must set the multipart boundary itself.
  const res = await fetch(`${API_BASE}${path}`, { method: 'POST', body: formData })
  if (!res.ok) throw new ApiError(res.status, path, await extractDetail(res))
  return res.json() as Promise<T>
}

/** Shape shared by every paginated list endpoint. */
interface Paged<T> {
  tracks?: T[]
  total?: number
}

/**
 * Fetch every page of a list endpoint.
 *
 * `/tracks` caps `per_page` at 200, but several pages want the whole result set
 * so they can sort and filter client-side. Asking for more than the cap is a 422,
 * and asking for exactly the cap silently truncates — both were live bugs. Page
 * through instead, with a hard stop so a misreported `total` cannot spin forever.
 */
export async function apiFetchAll<T>(
  path: string,
  { perPage = 200, maxPages = 25 }: { perPage?: number; maxPages?: number } = {},
): Promise<{ items: T[]; total: number; truncated: boolean }> {
  const sep = path.includes('?') ? '&' : '?'
  const items: T[] = []
  let total = 0

  for (let page = 1; page <= maxPages; page++) {
    const result = await apiFetch<Paged<T>>(`${path}${sep}page=${page}&per_page=${perPage}`)
    const batch = result.tracks ?? []
    total = result.total ?? items.length + batch.length
    items.push(...batch)
    if (batch.length < perPage || items.length >= total) {
      return { items, total, truncated: false }
    }
  }

  return { items, total, truncated: true }
}
