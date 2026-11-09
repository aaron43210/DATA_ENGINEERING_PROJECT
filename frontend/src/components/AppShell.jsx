import { NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext.jsx'

const link = ({ isActive }) =>
  `shrink-0 px-2 sm:px-3 py-1.5 rounded-md ${isActive ? 'bg-blue-50 text-blue-700 font-medium' : 'text-gray-700 hover:text-gray-900 hover:bg-gray-100'}`

export default function AppShell({ children }) {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  return (
    <div className="h-dvh flex flex-col bg-white text-gray-900">
      <nav className="shrink-0 bg-white border-b border-gray-200 px-3 sm:px-6 py-2 flex flex-wrap items-center justify-between gap-x-4 gap-y-1">
        <NavLink to="/dashboard" className="font-semibold text-gray-900">Geospatial Platform</NavLink>
        <div className="flex gap-1 items-center text-sm overflow-x-auto max-w-full">
          <NavLink to="/dashboard" className={link}>Catalog</NavLink>
          <NavLink to="/keys" className={link}>API Keys</NavLink>
          <NavLink to="/map" className={link}>Map</NavLink>
          {user?.role === 'admin' && <NavLink to="/admin" className={link}>Admin</NavLink>}
          {user?.email && <span className="ml-3 text-gray-500 hidden sm:inline">{user.email}</span>}
          <button onClick={async () => { await logout(); navigate('/') }} className="ml-auto sm:ml-2 shrink-0 px-3 py-1.5 rounded-md text-gray-700 hover:bg-gray-100">Sign out</button>
        </div>
      </nav>
      <div className="flex-1 min-h-0 overflow-auto">{children}</div>
    </div>
  )
}
