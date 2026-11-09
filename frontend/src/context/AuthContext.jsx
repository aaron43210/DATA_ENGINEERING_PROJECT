import { createContext, useContext, useState } from 'react'
import { api } from '../api/client.js'

const AuthContext = createContext(null)

// Only the non-secret profile is kept client-side; the session itself is an HttpOnly cookie.
localStorage.removeItem('gp_token')

export function AuthProvider({ children }) {
  const [user, setUser] = useState(() => {
    try { return JSON.parse(localStorage.getItem('gp_user')) } catch { return null }
  })

  function login(user) {
    localStorage.setItem('gp_user', JSON.stringify(user))
    setUser(user)
  }

  async function logout() {
    localStorage.removeItem('gp_user')
    setUser(null)
    await api.post('/auth/logout').catch(() => {})
  }

  return (
    <AuthContext.Provider value={{ user, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => useContext(AuthContext)
