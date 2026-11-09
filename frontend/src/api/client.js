const BASE = import.meta.env.VITE_API_BASE || '/api'

// The session is an HttpOnly cookie set by /auth/google, so scripts never see the token.
async function request(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...options.headers }
  const res = await fetch(`${BASE}${path}`, { ...options, headers, credentials: 'same-origin' })

  if (res.status === 401 && !path.startsWith('/auth/')) {
    localStorage.removeItem('gp_user')
    window.location.href = '/'
    return
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw Object.assign(new Error(err.detail || 'Request failed'), { status: res.status })
  }

  if (res.status === 204) return null
  return res.json()
}

export const api = {
  get: (path) => request(path),
  post: (path, body) => request(path, { method: 'POST', body: JSON.stringify(body) }),
  del: (path) => request(path, { method: 'DELETE' }),
}
