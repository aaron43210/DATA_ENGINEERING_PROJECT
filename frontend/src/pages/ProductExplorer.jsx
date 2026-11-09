import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client.js'
import { PRODUCTS, formatDate, toDate } from '../products.js'
import ApiUsage from '../components/ApiUsage.jsx'

const HIDDEN_COLUMNS = new Set(['geometry', 'footprint', 'tags'])
const PAGE_SIZE = 50

const label = c => c.replace(/_/g, ' ').replace(/\bpct\b/, '%').replace(/\bc\b$/, '°C').replace(/^\w/, ch => ch.toUpperCase())

function Cell({ column, value, spec }) {
  if (value == null) return <span className="text-gray-400">—</span>
  if (column === spec.dateField || column === 'event_time') return formatDate(value)
  if (column === 'scene_id') return <Link to={`/scene/${value}`} className="text-blue-600 hover:underline font-mono">{value}</Link>
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (typeof value === 'number') return Number.isInteger(value) ? value : value.toFixed(2)
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

export default function ProductExplorer() {
  const { product } = useParams()
  const spec = PRODUCTS[product]
  const [page, setPage] = useState(0)

  const { data, isLoading, error } = useQuery({
    queryKey: ['explore', product, page],
    queryFn: () => api.get(`${spec.endpoint}?limit=${PAGE_SIZE}&offset=${page * PAGE_SIZE}`),
    enabled: !!spec,
  })

  if (!spec) {
    return (
      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-6 sm:py-10">
        <Link to="/dashboard" className="text-gray-600 hover:text-gray-900 text-sm">← Catalog</Link>
        <p className="mt-4 text-gray-600">This product has no REST endpoint yet.</p>
      </div>
    )
  }

  const rows = Array.isArray(data) ? data : []
  const columns = rows.length > 0 ? Object.keys(rows[0]).filter(k => !HIDDEN_COLUMNS.has(k)) : []
  const dates = spec.dateField ? rows.map(r => r[spec.dateField]).filter(Boolean).map(toDate) : []
  const newest = dates.length ? new Date(Math.max(...dates)) : null
  const oldest = dates.length ? new Date(Math.min(...dates)) : null

  return (
    <div className="min-h-full bg-white text-gray-900 px-4 sm:px-6 py-6 sm:py-10">
      <div className="max-w-6xl mx-auto">
        <Link to="/dashboard" className="text-gray-600 hover:text-gray-900 text-sm">← Catalog</Link>
        <h2 className="text-2xl font-bold mt-2">{spec.title}</h2>
        <p className="text-gray-600 text-sm mt-1 mb-5">{spec.summary}</p>

        {newest && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-6">
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-3">
              <div className="text-xs text-gray-500">Latest {spec.dateLabel.toLowerCase()}</div>
              <div className="font-semibold">{newest.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })}</div>
            </div>
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-3">
              <div className="text-xs text-gray-500">Oldest on this page</div>
              <div className="font-semibold">{oldest.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })}</div>
            </div>
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-3">
              <div className="text-xs text-gray-500">Rows on this page</div>
              <div className="font-semibold">{rows.length}</div>
            </div>
          </div>
        )}

        {isLoading && <p className="text-gray-600">Loading…</p>}
        {error && <p className="text-red-600">Failed to load data.</p>}

        {rows.length > 0 && (
          <>
            {product === 'satellite_imagery' && (
              <p className="text-xs text-gray-500 mb-2">Click a scene ID for its full metadata and preview image.</p>
            )}
            <div className="overflow-x-auto rounded-xl border border-gray-200">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-gray-600">
                  <tr>
                    {columns.map(c => (
                      <th key={c} title={spec.fields[c]} className="px-3 py-2 text-left font-medium whitespace-nowrap">
                        {c === spec.dateField ? spec.dateLabel : label(c)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, i) => (
                    <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      {columns.map(c => (
                        <td key={c} className="px-3 py-2 text-gray-700 max-w-xs truncate whitespace-nowrap">
                          <Cell column={c} value={row[c]} spec={spec} />
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-xs text-gray-500 mt-2">Times are shown in your local time zone. The API returns UTC.</p>
          </>
        )}
        {(rows.length > 0 || page > 0) && (
          <div className="flex gap-3 mt-4 justify-end items-center">
            <button disabled={page === 0} onClick={() => setPage(p => p - 1)}
              className="text-sm text-gray-600 hover:text-gray-900 disabled:opacity-30">← Prev</button>
            <span className="text-sm text-gray-500">Page {page + 1}</span>
            <button disabled={rows.length < PAGE_SIZE} onClick={() => setPage(p => p + 1)}
              className="text-sm text-gray-600 hover:text-gray-900 disabled:opacity-30">Next →</button>
          </div>
        )}
        {!isLoading && !error && rows.length === 0 && <p className="text-gray-500">No data available.</p>}

        <ApiUsage spec={spec} />
      </div>
    </div>
  )
}
