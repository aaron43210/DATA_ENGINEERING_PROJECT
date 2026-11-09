import { useState } from 'react'

export default function KeyCreateModal({ rawKey, scopeLabel, expiresText, onClose }) {
  const [copied, setCopied] = useState(false)
  const [confirmed, setConfirmed] = useState(false)

  function copy() {
    navigator.clipboard.writeText(rawKey).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 px-4">
      <div className="bg-white border border-yellow-500 rounded-2xl p-6 max-w-md w-full shadow-2xl">
        <h3 className="text-lg font-bold text-gray-900 mb-1">Your new API key</h3>
        <p className="text-yellow-600 text-sm mb-4">
          ⚠ Store this now — it will <strong>not</strong> be shown again.
        </p>

        <p className="text-sm text-gray-700 mb-3">Access: <strong>{scopeLabel}</strong> · {expiresText}</p>

        <div className="bg-gray-100 rounded-lg px-4 py-3 font-mono text-sm text-green-700 break-all mb-3 select-all">
          {rawKey}
        </div>

        <button
          onClick={copy}
          className="w-full mb-4 bg-gray-200 hover:bg-gray-300 text-gray-900 py-2 rounded-lg text-sm font-medium transition-colors"
        >
          {copied ? '✓ Copied!' : 'Copy to clipboard'}
        </button>

        <label className="flex items-start gap-2 cursor-pointer mb-5">
          <input
            type="checkbox"
            className="mt-0.5"
            checked={confirmed}
            onChange={e => setConfirmed(e.target.checked)}
          />
          <span className="text-sm text-gray-700">I have saved my key in a secure location</span>
        </label>

        <button
          disabled={!confirmed}
          onClick={onClose}
          className="w-full bg-blue-600 hover:bg-blue-500 disabled:opacity-30 disabled:cursor-not-allowed text-white py-2 rounded-lg text-sm font-medium"
        >
          Done
        </button>
      </div>
    </div>
  )
}
