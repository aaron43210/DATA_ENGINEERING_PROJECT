import { Navigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext.jsx'
import AppShell from './AppShell.jsx'

export default function PrivateRoute({ children }) {
  const { user } = useAuth()
  return user ? <AppShell>{children}</AppShell> : <Navigate to="/" replace />
}
