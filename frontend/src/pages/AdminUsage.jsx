import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '../api/client.js'
import QuotaBar from '../components/QuotaBar.jsx'

export default function AdminUsage() {
  const { data: users = [], isLoading, error } = useQuery({
    queryKey: ['admin-usage'],
    queryFn: () => api.get('/admin/usage'),
  })

  return (
    <div className="min-h-full bg-white text-gray-900 px-6 py-10">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center gap-3 mb-6">
          <Link to="/dashboard" className="text-gray-600 hover:text-gray-900 text-sm">← Dashboard</Link>
          <h2 className="text-2xl font-bold">Usage — All Users</h2>
          <span className="ml-auto text-xs text-amber-600 bg-amber-400/10 px-2 py-0.5 rounded-full">Admin only</span>
        </div>

        {isLoading && <p className="text-gray-600">Loading…</p>}
        {error && <p className="text-red-600">Failed to load usage data.</p>}

        {users.length > 0 && (
          <div className="overflow-x-auto rounded-xl border border-gray-200">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-gray-600">
                <tr>
                  <th className="px-4 py-3 text-left">User</th>
                  <th className="px-4 py-3 text-left">Role</th>
                  <th className="px-4 py-3 text-left">Active keys</th>
                  <th className="px-4 py-3 text-left">Calls today</th>
                  <th className="px-4 py-3 text-left">Quota</th>
                </tr>
              </thead>
              <tbody>
                {users.map(u => (
                  <tr key={u.id} className="border-t border-gray-200 hover:bg-gray-50">
                    <td className="px-4 py-3">
                      <div className="font-medium">{u.name || '—'}</div>
                      <div className="text-gray-500 text-xs">{u.email}</div>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full ${u.role === 'admin' ? 'bg-amber-400/20 text-amber-700' : 'bg-gray-100 text-gray-700'}`}>
                        {u.role}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-gray-700">{u.key_count}</td>
                    <td className="px-4 py-3 text-gray-700">{u.total_calls_today}</td>
                    <td className="px-4 py-3 w-40">
                      <QuotaBar callsToday={Number(u.total_calls_today)} quotaDay={500 * Math.max(1, Number(u.key_count))} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {!isLoading && users.length === 0 && <p className="text-gray-500">No users yet.</p>}
      </div>
    </div>
  )
}
