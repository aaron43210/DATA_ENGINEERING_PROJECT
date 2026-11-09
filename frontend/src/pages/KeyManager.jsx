import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client.js'
import QuotaBar from '../components/QuotaBar.jsx'
import KeyCreateModal from '../components/KeyCreateModal.jsx'

const SCOPES = {
  all: { label: 'All products', hint: 'All 4 data products with one key', badge: 'bg-gray-900 text-white' },
  satellite_imagery: { label: 'Satellite Imagery Observations', hint: '/imagery', badge: 'bg-amber-100 text-amber-800' },
  osm_vector_product: { label: 'Kerala Critical Vector Infrastructure', hint: '/vector/features', badge: 'bg-blue-100 text-blue-800' },
  live_weather_stream: { label: 'Live Weather Stream Observations', hint: '/weather', badge: 'bg-emerald-100 text-emerald-800' },
  live_air_quality: { label: 'Live Air Quality Index', hint: '/air-quality', badge: 'bg-violet-100 text-violet-800' },
}

const EXPIRY = [
  { value: 7, label: '7 days' },
  { value: 30, label: '30 days' },
  { value: 90, label: '90 days' },
  { value: 365, label: '1 year' },
  { value: null, label: 'Never' },
]

function expiryText(expiresAt) {
  if (!expiresAt) return { text: 'Never expires', expired: false }
  const ms = new Date(expiresAt) - Date.now()
  const date = new Date(expiresAt).toLocaleDateString()
  if (ms <= 0) return { text: `Expired ${date}`, expired: true }
  const days = Math.ceil(ms / 86400000)
  return { text: `Expires ${date} (${days} day${days === 1 ? '' : 's'} left)`, expired: false }
}

export default function KeyManager() {
  const qc = useQueryClient()
  const [name, setName] = useState('')
  const [scope, setScope] = useState('all')
  const [expiry, setExpiry] = useState(30)
  const [created, setCreated] = useState(null)
  const [confirmRevoke, setConfirmRevoke] = useState(null)

  const { data: keys = [], isLoading } = useQuery({
    queryKey: ['keys'],
    queryFn: () => api.get('/keys'),
  })

  const createMutation = useMutation({
    mutationFn: () => api.post('/keys', { name: name.trim(), scope, expires_in_days: expiry }),
    onSuccess: (data) => { setCreated(data); setName(''); qc.invalidateQueries({ queryKey: ['keys'] }) },
  })

  const revokeMutation = useMutation({
    mutationFn: (prefix) => api.del(`/keys/${prefix}`),
    onSuccess: () => { setConfirmRevoke(null); qc.invalidateQueries({ queryKey: ['keys'] }) },
  })

  return (
    <div className="min-h-full bg-white text-gray-900">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 py-6 sm:py-10">
        <h2 className="text-2xl font-bold mb-6">API Keys</h2>

        <div className="bg-gray-50 rounded-xl p-5 border border-gray-200 mb-8 space-y-4">
          <h3 className="font-semibold">Create new key</h3>

          <input
            className="w-full bg-white border border-gray-300 text-gray-900 px-3 py-2 rounded-lg text-sm outline-none focus:ring-1 focus:ring-blue-500"
            placeholder="Key name (e.g. my-app)"
            value={name}
            maxLength={100}
            onChange={e => setName(e.target.value)}
          />

          <fieldset>
            <legend className="text-sm font-medium text-gray-700 mb-2">Access</legend>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {Object.entries(SCOPES).map(([id, s]) => (
                <label key={id} className={`flex items-start gap-2 p-3 rounded-lg border cursor-pointer ${scope === id ? 'border-blue-500 bg-blue-50' : 'border-gray-200 bg-white hover:border-gray-300'}`}>
                  <input type="radio" name="scope" className="mt-1" checked={scope === id} onChange={() => setScope(id)} />
                  <span>
                    <span className="block text-sm font-medium">{s.label}</span>
                    <span className="block text-xs text-gray-500">{s.hint}</span>
                  </span>
                </label>
              ))}
            </div>
          </fieldset>

          <div className="flex flex-wrap items-end gap-3">
            <label className="text-sm">
              <span className="block font-medium text-gray-700 mb-1">Expires after</span>
              <select
                className="bg-white border border-gray-300 px-3 py-2 rounded-lg text-sm"
                value={expiry ?? ''}
                onChange={e => setExpiry(e.target.value === '' ? null : Number(e.target.value))}
              >
                {EXPIRY.map(o => <option key={o.label} value={o.value ?? ''}>{o.label}</option>)}
              </select>
            </label>
            <button
              disabled={!name.trim() || createMutation.isPending}
              onClick={() => createMutation.mutate()}
              className="ml-auto bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-medium"
            >
              Generate key
            </button>
          </div>
          {createMutation.isError && <p className="text-sm text-red-600">Could not create key: {createMutation.error.message}</p>}
        </div>

        {isLoading ? <p className="text-gray-600">Loading…</p> : (
          <div className="space-y-3">
            {keys.length === 0 && <p className="text-gray-500 text-sm">No keys yet.</p>}
            {keys.map(k => {
              const s = SCOPES[k.scope] || SCOPES.all
              const exp = expiryText(k.expires_at)
              const usable = k.is_active && !exp.expired
              return (
                <div key={k.id} className={`bg-gray-50 rounded-xl p-4 border border-gray-200 ${usable ? '' : 'opacity-60'}`}>
                  <div className="flex items-center justify-between mb-2 gap-3">
                    <div className="flex items-center flex-wrap gap-2">
                      <span className="font-mono text-sm text-blue-700">{k.key_prefix}…</span>
                      <span className="text-sm text-gray-700">{k.name}</span>
                      <span className={`text-xs px-2 py-0.5 rounded-full ${s.badge}`}>{s.label}</span>
                      {!k.is_active && <span className="text-xs text-red-600 font-medium">Revoked</span>}
                      {k.is_active && exp.expired && <span className="text-xs text-red-600 font-medium">Expired</span>}
                    </div>
                    {k.is_active && (
                      <button onClick={() => setConfirmRevoke(k.key_prefix)} className="text-xs text-red-600 hover:text-red-800">Revoke</button>
                    )}
                  </div>
                  <QuotaBar callsToday={k.calls_today} quotaDay={k.quota_day} />
                  <p className="text-xs text-gray-500 mt-1">
                    {exp.text} · Created {new Date(k.created_at).toLocaleDateString()} · Last used {k.last_used_at ? new Date(k.last_used_at).toLocaleString() : 'never'}
                  </p>
                </div>
              )
            })}
          </div>
        )}
      </div>

      {created && <KeyCreateModal rawKey={created.key} scopeLabel={(SCOPES[created.scope] || SCOPES.all).label} expiresText={expiryText(created.expires_at).text} onClose={() => setCreated(null)} />}
      {confirmRevoke && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-6 max-w-sm w-full mx-4 border border-gray-200 shadow-xl">
            <p className="mb-4">Revoke key <span className="font-mono text-red-700">{confirmRevoke}…</span>? Apps using it will stop working.</p>
            <div className="flex gap-3 justify-end">
              <button onClick={() => setConfirmRevoke(null)} className="text-sm text-gray-600 hover:text-gray-900">Cancel</button>
              <button
                onClick={() => revokeMutation.mutate(confirmRevoke)}
                className="bg-red-600 hover:bg-red-500 text-white px-4 py-2 rounded-lg text-sm"
              >Revoke</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
