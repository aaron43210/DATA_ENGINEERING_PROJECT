import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { formatDate } from '../products.js'

const STAC_ITEM = 'https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a/items/'

const pct = v => `${Number(v).toFixed(1)}%`
const deg = v => `${Number(v).toFixed(1)}°`

const SECTIONS = [
  ['Dates', [
    ['datetime', 'Image captured', formatDate],
    ['created', 'Processed (record created)', formatDate],
    ['updated', 'Record last updated', formatDate],
  ]],
  ['Satellite and product', [
    ['platform', 'Satellite'],
    ['constellation', 'Constellation'],
    ['instruments', 'Instrument', v => v.join(', ').toUpperCase()],
    ['s2:product_type', 'Product type'],
    ['processing:level', 'Processing level'],
    ['s2:processing_baseline', 'Processing baseline'],
    ['grid:code', 'MGRS tile', v => v.replace('MGRS-', '')],
    ['proj:code', 'Projection'],
    ['proj:epsg', 'Projection EPSG'],
    ['s2:product_uri', 'ESA product'],
  ]],
  ['Scene conditions', [
    ['eo:cloud_cover', 'Cloud cover', pct],
    ['s2:cloud_shadow_percentage', 'Cloud shadow', pct],
    ['s2:thin_cirrus_percentage', 'Thin cirrus', pct],
    ['s2:vegetation_percentage', 'Vegetation', pct],
    ['s2:water_percentage', 'Water', pct],
    ['s2:not_vegetated_percentage', 'Bare / built-up', pct],
    ['s2:snow_ice_percentage', 'Snow and ice', pct],
    ['s2:dark_features_percentage', 'Dark features', pct],
    ['s2:nodata_pixel_percentage', 'No-data pixels', pct],
    ['view:sun_elevation', 'Sun elevation', deg],
    ['view:sun_azimuth', 'Sun azimuth', deg],
  ]],
]
const SHOWN = new Set(SECTIONS.flatMap(([, rows]) => rows.map(([k]) => k)))

function Row({ label, children }) {
  return (
    <div className="flex justify-between gap-4 py-2 border-b border-gray-100 text-sm">
      <span className="text-gray-500 shrink-0">{label}</span>
      <span className="text-gray-900 text-right break-all">{children}</span>
    </div>
  )
}

const show = v => (typeof v === 'object' ? JSON.stringify(v) : String(v))

function resolution(a) {
  if (a.gsd) return `${a.gsd} m`
  const r = a['raster:bands']?.[0]?.spatial_resolution
  return r ? `${r} m` : '—'
}

export default function SceneMetadata() {
  const { sceneId } = useParams()
  const { data: item, isLoading, error } = useQuery({
    queryKey: ['stac', sceneId],
    queryFn: () => fetch(STAC_ITEM + encodeURIComponent(sceneId)).then(r => {
      if (!r.ok) throw new Error(`STAC ${r.status}`)
      return r.json()
    }),
    staleTime: Infinity,
  })

  const p = item?.properties || {}
  const assets = Object.entries(item?.assets || {}).filter(([k]) => !k.endsWith('-jp2'))
  const others = Object.entries(p).filter(([k]) => !SHOWN.has(k))

  return (
    <div className="min-h-full bg-white text-gray-900 px-4 sm:px-6 py-6 sm:py-10">
      <div className="max-w-5xl mx-auto">
        <Link to="/explore/satellite_imagery" className="text-gray-600 hover:text-gray-900 text-sm">← Satellite Imagery Observations</Link>
        <h2 className="text-xl sm:text-2xl font-bold mt-2 break-all">{sceneId}</h2>
        <p className="text-sm text-gray-600 mt-1 mb-6">Sentinel-2 Level-2A scene metadata from the Earth Search STAC catalogue.</p>

        {isLoading && <div className="h-64 bg-gray-100 rounded-xl animate-pulse" />}
        {error && <p className="text-red-600">Could not load metadata for this scene.</p>}

        {item && (
          <>
            <div className="grid grid-cols-1 md:grid-cols-[minmax(0,2fr)_minmax(0,3fr)] gap-6">
              <div>
                {item.assets?.thumbnail?.href && (
                  <img src={item.assets.thumbnail.href} alt={`True-colour preview of ${sceneId}`} className="w-full rounded-xl border border-gray-200" />
                )}
                {item.bbox && (
                  <p className="text-xs text-gray-500 mt-2">Bounds (W, S, E, N): {item.bbox.map(v => v.toFixed(3)).join(', ')}</p>
                )}
              </div>
              <div className="space-y-6">
                {SECTIONS.map(([title, rows]) => {
                  const present = rows.filter(([k]) => p[k] != null)
                  if (!present.length) return null
                  return (
                    <section key={title}>
                      <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">{title}</h3>
                      {present.map(([k, lbl, fmt]) => <Row key={k} label={lbl}>{fmt ? fmt(p[k]) : show(p[k])}</Row>)}
                    </section>
                  )
                })}
              </div>
            </div>

            {assets.length > 0 && (
              <section className="mt-8">
                <h3 className="text-lg font-bold mb-2">Bands and files</h3>
                <div className="overflow-x-auto rounded-xl border border-gray-200">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 text-gray-600">
                      <tr><th className="px-3 py-2 text-left">Asset</th><th className="px-3 py-2 text-left">Description</th><th className="px-3 py-2 text-left">Resolution</th><th className="px-3 py-2 text-left">Format</th><th className="px-3 py-2 text-left">File</th></tr>
                    </thead>
                    <tbody>
                      {assets.map(([k, a]) => (
                        <tr key={k} className="border-t border-gray-200">
                          <td className="px-3 py-2 font-mono whitespace-nowrap">{k}</td>
                          <td className="px-3 py-2 text-gray-700 min-w-48">{a.title || '—'}</td>
                          <td className="px-3 py-2 text-gray-700 whitespace-nowrap">{resolution(a)}</td>
                          <td className="px-3 py-2 text-gray-600 whitespace-nowrap">{(a.type || '').split(';')[0].replace('image/', '').replace('application/', '') || '—'}</td>
                          <td className="px-3 py-2"><a href={a.href} target="_blank" rel="noreferrer" className="text-blue-600 hover:underline">Open</a></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            )}

            {others.length > 0 && (
              <details className="mt-8">
                <summary className="cursor-pointer font-semibold">All other properties ({others.length})</summary>
                <div className="mt-2">
                  {others.map(([k, v]) => <Row key={k} label={<span className="font-mono">{k}</span>}>{show(v)}</Row>)}
                </div>
              </details>
            )}

            <section className="mt-10 border-t border-gray-200 pt-8">
              <h3 className="text-lg font-bold mb-1">Access this scene</h3>
              <p className="text-sm text-gray-600 mb-3">
                List scenes from this platform with <code className="bg-gray-100 px-1 rounded">GET https://geoplatformkerala.tech/api/imagery/</code>{' '}
                (<Link to="/explore/satellite_imagery" className="text-blue-600 hover:underline">full API guide</Link>).
                The scene's full STAC record is public, no key needed:
              </p>
              <pre className="bg-gray-900 text-gray-100 text-xs sm:text-sm rounded-lg p-4 overflow-x-auto"><code>{`curl "${STAC_ITEM}${sceneId}"`}</code></pre>
            </section>
          </>
        )}
      </div>
    </div>
  )
}
