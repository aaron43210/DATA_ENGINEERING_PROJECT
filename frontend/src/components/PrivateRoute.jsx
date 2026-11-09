import { Navigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext.jsx'
import AppShell from './AppShell.jsx'

export default function PrivateRoute({ children }) {
  const { token } = useAuth()
  return token ? <AppShell>{children}</AppShell> : <Navigate to="/" replace />
}
