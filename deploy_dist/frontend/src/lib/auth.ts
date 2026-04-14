const TOKEN_KEY = 'aedmi_token'

/**
 * Persists the JWT token in localStorage.
 */
export function saveToken(token: string): void {
  if (typeof window !== 'undefined') {
    localStorage.setItem(TOKEN_KEY, token)
  }
}

/**
 * Retrieves the JWT token from localStorage.
 * Returns null if not found or running on the server.
 */
export function getToken(): string | null {
  if (typeof window === 'undefined') return null
  return localStorage.getItem(TOKEN_KEY)
}

/**
 * Removes the JWT token from localStorage.
 */
export function removeToken(): void {
  if (typeof window !== 'undefined') {
    localStorage.removeItem(TOKEN_KEY)
  }
}

/**
 * Returns true if a token is currently stored (user is authenticated).
 */
export function isAuthenticated(): boolean {
  return getToken() !== null
}
