import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '../api/client.js'
import { API_BASE_URL, PRODUCTS } from '../products.js'

export default function Dashboard() {
  const { data: products, isLoading, error } = useQuery({
    queryKey: ['datasets'],
    queryFn: () => api.get('/datasets/'),
  })

  return (
    <div className="min-h-full bg-white text-gray-900">
      <main className="max-w-5xl mx-auto px-4 sm:px-6 py-6 sm:py-10">
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
              <div className="text-xs text-gray-500">SLA: {p.sla_freshness_hours}h</div>
              {p.output_port_rest && (
                <Link to={`/explore/${p.product_id}`}
                  className="mt-2 text-sm text-blue-600 hover:text-blue-300">
                  Explore data →
                </Link>
              )}
            </div>
          ))}
        </div>

        <section className="mt-10 border-t border-gray-200 pt-8">
          <h3 className="text-lg font-bold mb-1">Use the API</h3>
          <p className="text-sm text-gray-600 mb-3">
            Base URL <code className="bg-gray-100 px-1 rounded break-all">{API_BASE_URL}</code>. Send your key in the{' '}
            <code className="bg-gray-100 px-1 rounded">X-API-Key</code> header (<Link to="/keys" className="text-blue-600 hover:underline">create one</Link>).
            Open a product above for its endpoint, parameters and examples. List this catalogue with:
          </p>
          <pre className="bg-gray-900 text-gray-100 text-xs sm:text-sm rounded-lg p-4 overflow-x-auto"><code>{`curl -H "X-API-Key: YOUR_API_KEY" "${API_BASE_URL}/datasets/"`}</code></pre>
          <div className="overflow-x-auto rounded-lg border border-gray-200 mt-4">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-gray-600">
                <tr><th className="px-3 py-2 text-left">Product</th><th className="px-3 py-2 text-left">Endpoint</th><th className="px-3 py-2 text-left">Key access needed</th></tr>
              </thead>
              <tbody>
                <tr className="border-t border-gray-200"><td className="px-3 py-2">Catalogue</td><td className="px-3 py-2 font-mono whitespace-nowrap">GET /datasets/</td><td className="px-3 py-2">Any key</td></tr>
                {(products || []).filter(p => PRODUCTS[p.product_id]).map(p => {
                  const s = PRODUCTS[p.product_id]
                  return (
                    <tr key={p.product_id} className="border-t border-gray-200">
                      <td className="px-3 py-2"><Link to={`/explore/${p.product_id}`} className="text-blue-600 hover:underline">{p.name}</Link></td>
                      <td className="px-3 py-2 font-mono whitespace-nowrap">GET {s.endpoint}</td>
                      <td className="px-3 py-2">{s.scope === 'all' ? 'All products' : `All products or ${s.title}`}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  )
}
