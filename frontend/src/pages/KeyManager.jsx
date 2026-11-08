import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client.js'
import QuotaBar from '../components/QuotaBar.jsx'
import KeyCreateModal from '../components/KeyCreateModal.jsx'

export default function KeyManager() {
  const qc = useQueryClient()
  const [newKeyName, setNewKeyName] = useState('')
  const [createdKey, setCreatedKey] = useState(null)
  const [confirmRevoke, setConfirmRevoke] = useState(null)

  const { data: keys = [], isLoading } = useQuery({
    queryKey: ['keys'],
    queryFn: () => api.get('/keys'),
  })

  const createMutation = useMutation({
    mutationFn: (name) => api.post('/keys', { name }),
    onSuccess: (data) => { setCreatedKey(data.key); setNewKeyName(''); qc.invalidateQueries(['keys']) },
  })

  const revokeMutation = useMutation({
    mutationFn: (prefix) => api.del(`/keys/${prefix}`),
    onSuccess: () => { setConfirmRevoke(null); qc.invalidateQueries(['keys']) },
  })

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      <div className="max-w-4xl mx-auto px-6 py-10">
        <h2 className="text-2xl font-bold mb-6">API Keys</h2>

        {/* Create form */}
        <div className="bg-gray-900 rounded-xl p-5 border border-gray-800 mb-8">
          <h3 className="font-semibold mb-3">Create new key</h3>
          <div className="flex gap-3">
            <input
              className="flex-1 bg-gray-800 text-white px-3 py-2 rounded-lg text-sm outline-none focus:ring-1 focus:ring-blue-500"
              placeholder="Key name (e.g. my-app)"
              value={newKeyName}
              onChange={e => setNewKeyName(e.target.value)}
            />
            <button
              disabled={!newKeyName.trim() || createMutation.isPending}
              onClick={() => createMutation.mutate(newKeyName.trim())}
              className="bg-blue-600 hover:bg-blue-500 disabled:opacity-40 px-4 py-2 rounded-lg text-sm font-medium"
            >
              Generate
            </button>
          </div>
        </div>

        {/* Key table */}
        {isLoading ? <p className="text-gray-400">Loading…</p> : (
          <div className="space-y-3">
            {keys.length === 0 && <p className="text-gray-500 text-sm">No keys yet.</p>}
            {keys.map(k => (
              <div key={k.id} className={`bg-gray-900 rounded-xl p-4 border ${k.is_active ? 'border-gray-800' : 'border-gray-700 opacity-50'}`}>
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <span className="font-mono text-sm text-blue-300">{k.key_prefix}…</span>
                    <span className="ml-3 text-sm text-gray-300">{k.name}</span>
                    {!k.is_active && <span className="ml-2 text-xs text-red-400">Revoked</span>}
                  </div>
                  {k.is_active && (
                    <button
                      onClick={() => setConfirmRevoke(k.key_prefix)}
                      className="text-xs text-red-400 hover:text-red-300"
                    >Revoke</button>
                  )}
                </div>
                <QuotaBar callsToday={k.calls_today} quotaDay={k.quota_day} />
                <p className="text-xs text-gray-500 mt-1">
                  Resets {k.quota_reset_at} · Last used {k.last_used_at ? new Date(k.last_used_at).toLocaleString() : 'never'}
                </p>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Modals */}
      {createdKey && <KeyCreateModal rawKey={createdKey} onClose={() => setCreatedKey(null)} />}
      {confirmRevoke && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
          <div className="bg-gray-900 rounded-xl p-6 max-w-sm w-full mx-4 border border-gray-700">
            <p className="mb-4">Revoke key <span className="font-mono text-red-300">{confirmRevoke}…</span>?</p>
            <div className="flex gap-3 justify-end">
              <button onClick={() => setConfirmRevoke(null)} className="text-sm text-gray-400 hover:text-white">Cancel</button>
              <button
                onClick={() => revokeMutation.mutate(confirmRevoke)}
                className="bg-red-600 hover:bg-red-500 px-4 py-2 rounded-lg text-sm"
              >Revoke</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
