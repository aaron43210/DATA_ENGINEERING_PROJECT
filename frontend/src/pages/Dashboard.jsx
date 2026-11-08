import { useQuery } from '@tanstack/react-query'
import { Link, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext.jsx'
import { api } from '../api/client.js'

function NavBar() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  return (
    <nav className="bg-gray-900 border-b border-gray-800 px-6 py-3 flex items-center justify-between">
      <span className="font-semibold text-white">Geospatial Platform</span>
      <div className="flex gap-4 items-center text-sm text-gray-300">
        <Link to="/dashboard" className="hover:text-white">Catalog</Link>
        <Link to="/keys" className="hover:text-white">API Keys</Link>
        <Link to="/map" className="hover:text-white">Map</Link>
        {user?.role === 'admin' && <Link to="/admin" className="hover:text-white text-amber-400">Admin</Link>}
        <button onClick={() => { logout(); navigate('/') }} className="hover:text-white">Sign out</button>
      </div>
    </nav>
  )
}

export default function Dashboard() {
  const { data: products, isLoading, error } = useQuery({
    queryKey: ['datasets'],
    queryFn: () => api.get('/datasets/'),
  })

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      <NavBar />
      <main className="max-w-5xl mx-auto px-6 py-10">
        <h2 className="text-2xl font-bold mb-6">Data Product Catalog</h2>
        {isLoading && <p className="text-gray-400">Loading…</p>}
        {error && <p className="text-red-400">Failed to load catalog.</p>}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
          {(products || []).map(p => (
            <div key={p.product_id} className="bg-gray-900 rounded-xl p-5 border border-gray-800 flex flex-col gap-2">
              <div className="flex items-start justify-between">
                <h3 className="font-semibold text-lg">{p.name}</h3>
                <span className="text-xs bg-gray-800 text-gray-300 px-2 py-0.5 rounded-full">{p.domain}</span>
              </div>
              <p className="text-gray-400 text-sm flex-1">{p.description}</p>
              <div className="text-xs text-gray-500">SLA: {p.sla_freshness_hours}h · {p.owner_team}</div>
              {p.output_port_rest && (
                <Link to={`/explore/${p.product_id}`}
                  className="mt-2 text-sm text-blue-400 hover:text-blue-300">
                  Explore data →
                </Link>
              )}
            </div>
          ))}
        </div>
      </main>
    </div>
  )
}
