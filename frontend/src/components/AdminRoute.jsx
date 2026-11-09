import { Navigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext.jsx'
import AppShell from './AppShell.jsx'

export default function AdminRoute({ children }) {
  const { token, user } = useAuth()
  if (!token) return <Navigate to="/" replace />
  if (user?.role !== 'admin') return <Navigate to="/dashboard" replace />
  return <AppShell>{children}</AppShell>
}
