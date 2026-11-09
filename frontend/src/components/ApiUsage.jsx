import { useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE_URL } from '../products.js'

function Code({ children }) {
  const [copied, setCopied] = useState(false)
  function copy() {
    navigator.clipboard.writeText(children).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    })
  }
  return (
    <div className="relative">
      <pre className="bg-gray-900 text-gray-100 text-xs sm:text-sm rounded-lg p-4 pr-16 overflow-x-auto"><code>{children}</code></pre>
      <button onClick={copy} className="absolute top-2 right-2 text-xs bg-gray-700 hover:bg-gray-600 text-white px-2 py-1 rounded">
        {copied ? 'Copied' : 'Copy'}
      </button>
    </div>
  )
}

const ERRORS = [
  ['401', 'Missing, invalid, revoked or expired API key'],
  ['403', 'Key is scoped to a different product'],
  ['422', 'A query parameter has the wrong type, e.g. a bad date'],
  ['429', 'Daily quota used up (500 calls per key), resets at midnight UTC'],
]

export default function ApiUsage({ spec }) {
  const url = `${API_BASE_URL}${spec.endpoint}${spec.example}`
  const scopeText = spec.scope === 'all'
    ? <>a key with <strong>All products</strong> access</>
    : <>a key with <strong>All products</strong> or <strong>{spec.title}</strong> access</>

  return (
    <section className="mt-10 border-t border-gray-200 pt-8">
      <h3 className="text-lg font-bold mb-1">Access this data with the API</h3>
      <p className="text-sm text-gray-600 mb-5">
        Send your API key in the <code className="bg-gray-100 px-1 rounded">X-API-Key</code> header. You need {scopeText}.{' '}
        <Link to="/keys" className="text-blue-600 hover:underline">Create a key</Link>.
      </p>

      <dl className="grid grid-cols-1 sm:grid-cols-[8rem_1fr] gap-x-4 gap-y-2 text-sm mb-6">
        <dt className="text-gray-500">Method</dt><dd className="font-mono">GET</dd>
        <dt className="text-gray-500">Endpoint</dt><dd className="font-mono break-all">{API_BASE_URL}{spec.endpoint}</dd>
        <dt className="text-gray-500">Header</dt><dd className="font-mono break-all">X-API-Key: YOUR_API_KEY</dd>
        <dt className="text-gray-500">Response</dt><dd>JSON array of records, newest first</dd>
      </dl>

      <h4 className="font-semibold text-sm mb-2">Query parameters (all optional)</h4>
      <div className="overflow-x-auto rounded-lg border border-gray-200 mb-6">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-gray-600">
            <tr><th className="px-3 py-2 text-left">Name</th><th className="px-3 py-2 text-left">Type</th><th className="px-3 py-2 text-left">Default</th><th className="px-3 py-2 text-left">Description</th></tr>
          </thead>
          <tbody>
            {spec.params.map(p => (
              <tr key={p.name} className="border-t border-gray-200">
                <td className="px-3 py-2 font-mono whitespace-nowrap">{p.name}</td>
                <td className="px-3 py-2 text-gray-600 whitespace-nowrap">{p.type}</td>
                <td className="px-3 py-2 font-mono text-gray-600">{p.default ?? '—'}</td>
                <td className="px-3 py-2 text-gray-700 min-w-48">{p.desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h4 className="font-semibold text-sm mb-2">curl</h4>
      <Code>{`curl -H "X-API-Key: YOUR_API_KEY" \\\n  "${url}"`}</Code>

      <h4 className="font-semibold text-sm mt-5 mb-2">Python</h4>
      <Code>{`import requests

r = requests.get(
    "${API_BASE_URL}${spec.endpoint}",
    headers={"X-API-Key": "YOUR_API_KEY"},
    params={${spec.example.slice(1).split('&').map(kv => { const [k, v] = kv.split('='); return `"${k}": "${v}"` }).join(', ')}},
)
r.raise_for_status()
rows = r.json()`}</Code>

      <h4 className="font-semibold text-sm mt-5 mb-2">JavaScript</h4>
      <Code>{`const res = await fetch("${url}", {
  headers: { "X-API-Key": "YOUR_API_KEY" },
})
const rows = await res.json()`}</Code>

      <h4 className="font-semibold text-sm mt-6 mb-2">Response fields</h4>
      <div className="overflow-x-auto rounded-lg border border-gray-200 mb-6">
        <table className="w-full text-sm">
          <tbody>
            {Object.entries(spec.fields).map(([k, v]) => (
              <tr key={k} className="border-t first:border-t-0 border-gray-200">
                <td className="px-3 py-2 font-mono whitespace-nowrap">{k}</td>
                <td className="px-3 py-2 text-gray-700">{v}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h4 className="font-semibold text-sm mb-2">Errors</h4>
      <ul className="text-sm space-y-1">
        {ERRORS.map(([code, text]) => (
          <li key={code}><span className="font-mono font-semibold mr-2">{code}</span><span className="text-gray-700">{text}</span></li>
        ))}
      </ul>
    </section>
  )
}
