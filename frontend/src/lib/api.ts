import { getToken, removeToken } from './auth'

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

/**
 * Base fetch wrapper that:
 * - Attaches the JWT token from localStorage as Bearer header
 * - Redirects to /login on 401 responses
 * - Throws ApiError for non-2xx responses
 */
export async function apiFetch<T = unknown>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const token = getToken()

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  }

  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }

  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
  })

  if (response.status === 401) {
    removeToken()
    if (typeof window !== 'undefined') {
      window.location.href = '/login'
    }
    throw new ApiError(401, 'No autorizado — redirigiendo a login')
  }

  if (!response.ok) {
    let message = `Error ${response.status}`
    try {
      const body = await response.json()
      message = body?.detail ?? body?.message ?? message
    } catch {
      // ignore parse errors
    }
    throw new ApiError(response.status, message)
  }

  // Handle empty responses (e.g. 204 No Content)
  const contentType = response.headers.get('content-type')
  if (contentType && contentType.includes('application/json')) {
    return response.json() as Promise<T>
  }

  return undefined as T
}
