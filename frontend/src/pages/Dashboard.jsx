import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '../api/client.js'

export default function Dashboard() {
  const { data: products, isLoading, error } = useQuery({
    queryKey: ['datasets'],
    queryFn: () => api.get('/datasets/'),
  })

  return (
    <div className="min-h-full bg-white text-gray-900">
      <main className="max-w-5xl mx-auto px-6 py-10">
        <h2 className="text-2xl font-bold mb-6">Data Product Catalog</h2>
        {isLoading && <p className="text-gray-600">Loading…</p>}
        {error && <p className="text-red-600">Failed to load catalog.</p>}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
          {(products || []).map(p => (
            <div key={p.product_id} className="bg-gray-50 rounded-xl p-5 border border-gray-200 flex flex-col gap-2">
              <div className="flex items-start justify-between">
                <h3 className="font-semibold text-lg">{p.name}</h3>
                <span className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded-full">{p.domain}</span>
              </div>
              <p className="text-gray-600 text-sm flex-1">{p.description}</p>
              <div className="text-xs text-gray-500">SLA: {p.sla_freshness_hours}h · {p.owner_team}</div>
              {p.output_port_rest && (
                <Link to={`/explore/${p.product_id}`}
                  className="mt-2 text-sm text-blue-600 hover:text-blue-300">
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
