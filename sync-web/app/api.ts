const API_BASE = '/api'

export async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })
  if (!res.ok) {
    let detail = `API error: ${res.status}`
    try {
      const body = await res.json()
      if (body?.detail) detail = body.detail
      else if (body?.message) detail = body.message
      else if (body?.error) detail = body.error
    } catch {}
    throw new Error(detail)
  }
  return res.json()
}

export async function apiUpload<T>(path: string, formData: FormData): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    body: formData,
  })
  if (!res.ok) {
    let detail = `Upload error: ${res.status}`
    try {
      const body = await res.json()
      if (body?.detail) detail = body.detail
      else if (body?.message) detail = body.message
      else if (body?.error) detail = body.error
    } catch {}
    throw new Error(detail)
  }
  return res.json()
}
