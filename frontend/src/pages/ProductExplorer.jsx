import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client.js'

const PRODUCT_ENDPOINTS = {
  osm_vector_product: '/vector/features/',
  satellite_imagery: '/imagery/',
  weather_data: '/weather/',
  climate_spatial_mart: '/datasets/',
  iot_sensor_stream: '/sensors/',
  live_weather_stream: '/weather/',
}

export default function ProductExplorer() {
  const { product } = useParams()
  const [page, setPage] = useState(0)
  const endpoint = PRODUCT_ENDPOINTS[product] || `/datasets/`
  const PAGE_SIZE = 50

  const { data, isLoading, error } = useQuery({
    queryKey: ['explore', product, page],
    queryFn: () => api.get(`${endpoint}?limit=${PAGE_SIZE}&offset=${page * PAGE_SIZE}`),
  })

  const rows = Array.isArray(data) ? data : data?.features?.map(f => ({ ...f.properties, _geometry: f.geometry?.type })) || []
  const columns = rows.length > 0 ? Object.keys(rows[0]).filter(k => k !== '_geometry') : []

  return (
    <div className="min-h-screen bg-gray-950 text-white px-6 py-10">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center gap-3 mb-6">
          <Link to="/dashboard" className="text-gray-400 hover:text-white text-sm">← Catalog</Link>
          <h2 className="text-xl font-bold">{product}</h2>
        </div>

        {isLoading && <p className="text-gray-400">Loading…</p>}
        {error && <p className="text-red-400">Failed to load data.</p>}

        {rows.length > 0 && (
          <>
            <div className="overflow-x-auto rounded-xl border border-gray-800">
              <table className="w-full text-sm">
                <thead className="bg-gray-900 text-gray-400">
                  <tr>{columns.map(c => <th key={c} className="px-3 py-2 text-left font-medium">{c}</th>)}</tr>
                </thead>
                <tbody>
                  {rows.map((row, i) => (
                    <tr key={i} className={i % 2 === 0 ? 'bg-gray-950' : 'bg-gray-900/40'}>
                      {columns.map(c => (
                        <td key={c} className="px-3 py-2 text-gray-300 max-w-xs truncate">
                          {row[c] == null ? <span className="text-gray-600">—</span> : String(row[c])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="flex gap-3 mt-4 justify-end">
              <button disabled={page === 0} onClick={() => setPage(p => p - 1)}
                className="text-sm text-gray-400 hover:text-white disabled:opacity-30">← Prev</button>
              <span className="text-sm text-gray-500">Page {page + 1}</span>
              <button disabled={rows.length < PAGE_SIZE} onClick={() => setPage(p => p + 1)}
                className="text-sm text-gray-400 hover:text-white disabled:opacity-30">Next →</button>
            </div>
          </>
        )}
        {!isLoading && rows.length === 0 && <p className="text-gray-500">No data available.</p>}
      </div>
    </div>
  )
}
