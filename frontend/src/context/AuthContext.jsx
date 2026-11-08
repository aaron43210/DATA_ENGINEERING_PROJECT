import { createContext, useContext, useState } from 'react'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [user, setUser] = useState(() => {
    try { return JSON.parse(localStorage.getItem('gp_user')) } catch { return null }
  })
  const [token, setToken] = useState(() => localStorage.getItem('gp_token'))

  function login(token, user) {
    localStorage.setItem('gp_token', token)
    localStorage.setItem('gp_user', JSON.stringify(user))
    setToken(token)
    setUser(user)
  }

  function logout() {
    localStorage.removeItem('gp_token')
    localStorage.removeItem('gp_user')
    setToken(null)
    setUser(null)
  }

  return (
    <AuthContext.Provider value={{ user, token, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => useContext(AuthContext)
