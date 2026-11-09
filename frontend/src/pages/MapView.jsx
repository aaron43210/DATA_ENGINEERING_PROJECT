import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import * as maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?url'
import { api } from '../api/client.js'

// MapLibre resolves its worker next to its own module URL, which Vite never emits.
maplibregl.setWorkerUrl(workerUrl)

const KERALA_CENTER = [76.3, 10.5]
const KERALA_ZOOM = 7
const VECTOR_TYPES = ['hospital', 'clinic', 'ambulance_station', 'highway_trunk']
const STAC_ITEM = 'https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a/items/'
const TYPE_LABELS = {
  hospital: 'Hospital', clinic: 'Clinic', doctors: 'Doctors', ambulance_station: 'Ambulance station',
  highway_trunk: 'Trunk road', highway_primary: 'Primary road',
}
const QUALITY_BADGE = { Excellent: 'bg-emerald-100 text-emerald-800', Good: 'bg-blue-100 text-blue-800', Poor: 'bg-gray-200 text-gray-700' }
// OpenWeather AQI 1..5
const AQI_COLORS = { 1: '#22c55e', 2: '#eab308', 3: '#f97316', 4: '#ef4444', 5: '#7e22ce' }
const POLLUTANTS = [['pm2_5', 'PM2.5'], ['pm10', 'PM10'], ['no2', 'NO₂'], ['o3', 'O₃'], ['so2', 'SO₂'], ['co', 'CO'], ['nh3', 'NH₃'], ['no', 'NO']]
const LAYER_IDS = {
  vector: ['vector-points', 'vector-lines'],
  imagery: ['imagery-fills', 'imagery-outlines'],
  air: ['air-points'],
}

const empty = { type: 'FeatureCollection', features: [] }

// Postgres text timestamps ("2026-11-06 05:36:11+00") are not ISO; Safari rejects them.
const toDate = t => new Date(t.replace(' ', 'T').replace(/([+-]\d\d)$/, '$1:00'))

function lineLengthKm(coords) {
  let km = 0
  for (let i = 1; i < coords.length; i++) {
    const [lon1, lat1] = coords[i - 1], [lon2, lat2] = coords[i]
    const r = Math.PI / 180
    const a = Math.sin((lat2 - lat1) * r / 2) ** 2 + Math.cos(lat1 * r) * Math.cos(lat2 * r) * Math.sin((lon2 - lon1) * r / 2) ** 2
    km += 12742 * Math.asin(Math.sqrt(a))
  }
  return km
}

function Row({ label, children }) {
  return (
    <div className="flex justify-between gap-4 py-1.5 border-b border-gray-100 text-sm">
      <span className="text-gray-500 shrink-0">{label}</span>
      <span className="text-gray-900 text-right break-all">{children}</span>
    </div>
  )
}

function VectorDetail({ f }) {
  const g = f.geometry
  const tags = Object.entries(f.tags || {}).filter(([k]) => k !== 'name').sort(([a], [b]) => a.localeCompare(b))
  return (
    <>
      <span className="inline-block text-xs px-2 py-0.5 rounded-full bg-blue-100 text-blue-800 mb-2">{TYPE_LABELS[f.feature_type] || f.feature_type}</span>
      <h3 className="text-lg font-semibold mb-3">{f.name || 'Unnamed feature'}</h3>
      {g?.type === 'Point' && <Row label="Coordinates">{g.coordinates[1].toFixed(5)}, {g.coordinates[0].toFixed(5)}</Row>}
      {g?.type === 'LineString' && <Row label="Length">{lineLengthKm(g.coordinates).toFixed(2)} km</Row>}
      <Row label="OSM ID">
        <a className="text-blue-600 hover:underline" target="_blank" rel="noreferrer"
          href={`https://www.openstreetmap.org/${f.osm_type || 'node'}/${f.osm_id}`}>{f.osm_type || 'node'}/{f.osm_id} ↗</a>
      </Row>
      {g?.type === 'Point' && (
        <Row label="Directions">
          <a className="text-blue-600 hover:underline" target="_blank" rel="noreferrer"
            href={`https://www.google.com/maps/dir/?api=1&destination=${g.coordinates[1]},${g.coordinates[0]}`}>Google Maps ↗</a>
        </Row>
      )}
      {tags.length > 0 && (
        <>
          <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mt-5 mb-1">OSM tags</h4>
          {tags.map(([k, v]) => <Row key={k} label={k}>{String(v)}</Row>)}
        </>
      )}
    </>
  )
}

function ImageryDetail({ scenes, index, onPick }) {
  const s = scenes[index]
  const { data: item, isLoading } = useQuery({
    queryKey: ['stac', s.scene_id],
    queryFn: () => fetch(STAC_ITEM + s.scene_id).then(r => (r.ok ? r.json() : null)),
    staleTime: Infinity,
  })
  const p = item?.properties || {}
  const pct = v => (v == null ? '—' : `${Number(v).toFixed(1)}%`)
  const [w, so, e, n] = [s.footprint.coordinates[0][0][0], s.footprint.coordinates[0][0][1], s.footprint.coordinates[0][2][0], s.footprint.coordinates[0][2][1]]
  return (
    <>
      {scenes.length > 1 && (
        <div className="mb-3">
          <p className="text-xs text-gray-500 mb-1">{scenes.length} scenes cover this spot</p>
          <div className="flex flex-wrap gap-1">
            {scenes.map((x, i) => (
              <button key={x.scene_id} onClick={() => onPick(i)}
                className={`text-xs px-2 py-1 rounded border ${i === index ? 'border-blue-500 bg-blue-50 text-blue-700' : 'border-gray-200 hover:bg-gray-50'}`}>
                {toDate(x.acquisition_time).toLocaleDateString()}
              </button>
            ))}
          </div>
        </div>
      )}
      <span className={`inline-block text-xs px-2 py-0.5 rounded-full mb-2 ${QUALITY_BADGE[s.image_quality] || QUALITY_BADGE.Poor}`}>{s.image_quality} image</span>
      <h3 className="text-base font-semibold mb-3 break-all">{s.scene_id}</h3>
      {isLoading && <div className="w-full aspect-square bg-gray-100 rounded-lg animate-pulse mb-3" />}
      {item?.assets?.thumbnail?.href && (
        <img src={item.assets.thumbnail.href} alt={`Preview of ${s.scene_id}`} className="w-full rounded-lg border border-gray-200 mb-3" />
      )}
      <Row label="Satellite">{s.satellite}</Row>
      <Row label="Acquired">{toDate(s.acquisition_time).toLocaleString()}</Row>
      <Row label="Cloud cover">{pct(s.cloud_percentage)}</Row>
      {p['grid:code'] && <Row label="Tile">{p['grid:code'].replace('MGRS-', '')}</Row>}
      {p['view:sun_elevation'] != null && <Row label="Sun elevation">{p['view:sun_elevation'].toFixed(1)}°</Row>}
      {p['s2:vegetation_percentage'] != null && <Row label="Vegetation">{pct(p['s2:vegetation_percentage'])}</Row>}
      {p['s2:water_percentage'] != null && <Row label="Water">{pct(p['s2:water_percentage'])}</Row>}
      {p['s2:nodata_pixel_percentage'] != null && <Row label="No-data pixels">{pct(p['s2:nodata_pixel_percentage'])}</Row>}
      <Row label="Bounds (W,S,E,N)">{[w, so, e, n].map(v => v.toFixed(3)).join(', ')}</Row>
      <a className="inline-block mt-4 text-sm text-blue-600 hover:underline" target="_blank" rel="noreferrer" href={STAC_ITEM + s.scene_id}>Full STAC metadata ↗</a>
    </>
  )
}

function AirDetail({ s }) {
  return (
    <>
      <span className="inline-block text-xs px-2 py-0.5 rounded-full mb-2 text-white" style={{ background: AQI_COLORS[s.aqi] }}>
        AQI {s.aqi} · {s.aqi_label}
      </span>
      <h3 className="text-lg font-semibold mb-3">{s.city_name || s.station_id}</h3>
      <Row label="Station">{s.station_id}</Row>
      <Row label="Measured">{toDate(s.event_time).toLocaleString()}</Row>
      <Row label="Coordinates">{s.latitude.toFixed(4)}, {s.longitude.toFixed(4)}</Row>
      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mt-5 mb-1">Pollutants <span className="normal-case">(µg/m³)</span></h4>
      {POLLUTANTS.map(([k, label]) => <Row key={k} label={label}>{s[k] == null ? '—' : s[k].toFixed(2)}</Row>)}
      <p className="text-xs text-gray-500 mt-4">Scale: 1 Good, 2 Fair, 3 Moderate, 4 Poor, 5 Very Poor. Source: OpenWeather, refreshed hourly.</p>
    </>
  )
}

export default function MapView() {
  const mapContainer = useRef(null)
  const map = useRef(null)
  const vectorRows = useRef([])
  const imageryRows = useRef([])
  const airRows = useRef([])
  const [layers, setLayers] = useState({ vector: true, imagery: false, air: true })
  const [status, setStatus] = useState('')
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    if (map.current) return
    const m = new maplibregl.Map({
      container: mapContainer.current,
      style: {
        version: 8,
        sources: {
          osm: {
            type: 'raster',
            tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '© OpenStreetMap contributors',
          },
        },
        layers: [{ id: 'osm-tiles', type: 'raster', source: 'osm' }],
      },
      center: KERALA_CENTER,
      zoom: KERALA_ZOOM,
    })
    map.current = m
    m.addControl(new maplibregl.NavigationControl(), 'top-right')

    m.on('load', () => {
      m.addSource('selected', { type: 'geojson', data: empty })
      m.addLayer({ id: 'selected-fill', type: 'fill', source: 'selected', filter: ['==', ['geometry-type'], 'Polygon'],
        paint: { 'fill-color': '#2563eb', 'fill-opacity': 0.15 } })
      m.addLayer({ id: 'selected-line', type: 'line', source: 'selected',
        paint: { 'line-color': '#2563eb', 'line-width': 4 } })
      m.addLayer({ id: 'selected-point', type: 'circle', source: 'selected', filter: ['==', ['geometry-type'], 'Point'],
        paint: { 'circle-radius': 9, 'circle-color': '#2563eb', 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 } })
      loadVectorLayer().then(loadAirLayer)
    })

    m.on('click', (e) => {
      const near = [[e.point.x - 5, e.point.y - 5], [e.point.x + 5, e.point.y + 5]]
      const has = id => m.getLayer(id) && m.getLayoutProperty(id, 'visibility') !== 'none'
      const station = has('air-points') ? m.queryRenderedFeatures(near, { layers: ['air-points'] })[0] : null
      if (station) {
        setSelected({ kind: 'air', row: airRows.current[station.properties.i] })
        return
      }
      const vectorLayers = ['vector-points', 'vector-lines'].filter(has)
      const hit = vectorLayers.length ? m.queryRenderedFeatures(near, { layers: vectorLayers }) : []
      const hitPoint = hit.find(f => f.layer.id === 'vector-points') || hit[0]
      if (hitPoint) {
        setSelected({ kind: 'vector', row: vectorRows.current[hitPoint.properties.i] })
        return
      }
      const scenes = has('imagery-fills') ? m.queryRenderedFeatures(e.point, { layers: ['imagery-fills'] }) : []
      if (scenes.length) {
        const rows = [...new Set(scenes.map(f => f.properties.i))].map(i => imageryRows.current[i])
          .sort((a, b) => b.acquisition_time.localeCompare(a.acquisition_time))
        setSelected({ kind: 'imagery', rows, index: 0 })
        return
      }
      setSelected(null)
    })

    const pointer = (on) => () => { m.getCanvas().style.cursor = on ? 'pointer' : '' }
    for (const id of ['vector-points', 'vector-lines', 'imagery-fills', 'air-points']) {
      m.on('mouseenter', id, pointer(true))
      m.on('mouseleave', id, pointer(false))
    }
  }, [])

  useEffect(() => {
    const src = map.current?.getSource('selected')
    if (!src) return
    const geom = selected?.kind === 'vector' ? selected.row.geometry
      : selected?.kind === 'imagery' ? selected.rows[selected.index].footprint
      : selected?.kind === 'air' ? { type: 'Point', coordinates: [selected.row.longitude, selected.row.latitude] } : null
    src.setData(geom ? { type: 'Feature', geometry: geom, properties: {} } : empty)
  }, [selected])

  async function loadVectorLayer() {
    const m = map.current
    if (!m) return
    setStatus('Loading vector features…')
    try {
      const data = (await Promise.all(VECTOR_TYPES.map(t => api.get(`/vector/features?feature_type=${t}&limit=500`)))).flat()
      vectorRows.current = data
      const geojson = {
        type: 'FeatureCollection',
        features: data.map((f, i) => ({ type: 'Feature', geometry: f.geometry, properties: { i } })),
      }
      if (m.getSource('vector-features')) {
        m.getSource('vector-features').setData(geojson)
      } else {
        m.addSource('vector-features', { type: 'geojson', data: geojson })
        m.addLayer({ id: 'vector-lines', type: 'line', source: 'vector-features',
          filter: ['in', ['geometry-type'], ['literal', ['LineString', 'MultiLineString']]],
          paint: { 'line-color': '#10b981', 'line-width': 2.5 } }, 'selected-fill')
        m.addLayer({ id: 'vector-points', type: 'circle', source: 'vector-features',
          filter: ['==', ['geometry-type'], 'Point'],
          paint: { 'circle-radius': 5, 'circle-color': '#3b82f6', 'circle-opacity': 0.85, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1 } }, 'selected-fill')
      }
      setStatus(`${data.length} features · click one for details`)
    } catch {
      setStatus('Vector features unavailable')
    }
  }

  async function loadImageryLayer() {
    const m = map.current
    if (!m) return
    setStatus('Loading imagery footprints…')
    try {
      const data = (await api.get('/imagery/?limit=200')).filter(s => s.footprint)
      imageryRows.current = data
      const geojson = {
        type: 'FeatureCollection',
        features: data.map((s, i) => ({ type: 'Feature', geometry: s.footprint, properties: { i } })),
      }
      if (m.getSource('imagery')) {
        m.getSource('imagery').setData(geojson)
      } else {
        // Footprints sit under the vector layers so points stay clickable.
        const below = m.getLayer('vector-lines') ? 'vector-lines' : 'selected-fill'
        m.addSource('imagery', { type: 'geojson', data: geojson })
        m.addLayer({ id: 'imagery-fills', type: 'fill', source: 'imagery',
          paint: { 'fill-color': '#f59e0b', 'fill-opacity': 0.15 } }, below)
        m.addLayer({ id: 'imagery-outlines', type: 'line', source: 'imagery',
          paint: { 'line-color': '#f59e0b', 'line-width': 1 } }, below)
      }
      setStatus(`${data.length} imagery footprints · click one for details`)
    } catch {
      setStatus('Imagery unavailable')
    }
  }

  async function loadAirLayer() {
    const m = map.current
    if (!m) return
    try {
      const data = await api.get('/air-quality/?latest=true&limit=100')
      airRows.current = data
      const geojson = {
        type: 'FeatureCollection',
        features: data.map((s, i) => ({ type: 'Feature', geometry: { type: 'Point', coordinates: [s.longitude, s.latitude] }, properties: { i, aqi: s.aqi } })),
      }
      if (m.getSource('air')) {
        m.getSource('air').setData(geojson)
      } else {
        m.addSource('air', { type: 'geojson', data: geojson })
        m.addLayer({ id: 'air-points', type: 'circle', source: 'air',
          paint: {
            'circle-radius': 11,
            'circle-color': ['match', ['get', 'aqi'], 1, AQI_COLORS[1], 2, AQI_COLORS[2], 3, AQI_COLORS[3], 4, AQI_COLORS[4], AQI_COLORS[5]],
            'circle-opacity': 0.9, 'circle-stroke-color': '#fff', 'circle-stroke-width': 2,
          } }, 'selected-fill')
      }
    } catch {
      setStatus('Air quality unavailable')
    }
  }

  const LOADERS = { vector: loadVectorLayer, imagery: loadImageryLayer, air: loadAirLayer }

  function toggleLayer(name) {
    const next = !layers[name]
    setLayers(l => ({ ...l, [name]: next }))
    const m = map.current
    if (!m) return
    const vis = next ? 'visible' : 'none'
    LAYER_IDS[name].forEach(id => m.getLayer(id) && m.setLayoutProperty(id, 'visibility', vis))
    if (!next && selected?.kind === name) setSelected(null)
    if (next) LOADERS[name]()
  }

  return (
    <div className="h-full flex flex-col bg-white">
      <div className="flex items-center gap-4 px-4 py-2 bg-gray-50 border-b border-gray-200 text-sm text-gray-900">
        <label className="flex items-center gap-1.5 cursor-pointer">
          <input type="checkbox" checked={layers.vector} onChange={() => toggleLayer('vector')} />
          <span className="w-2.5 h-2.5 rounded-full bg-blue-500" />
          <span>Hospitals, clinics &amp; trunk roads</span>
        </label>
        <label className="flex items-center gap-1.5 cursor-pointer">
          <input type="checkbox" checked={layers.imagery} onChange={() => toggleLayer('imagery')} />
          <span className="w-2.5 h-2.5 rounded-sm bg-amber-400" />
          <span>Satellite footprints</span>
        </label>
        <label className="flex items-center gap-1.5 cursor-pointer">
          <input type="checkbox" checked={layers.air} onChange={() => toggleLayer('air')} />
          <span className="w-2.5 h-2.5 rounded-full" style={{ background: 'linear-gradient(90deg,#22c55e,#eab308,#ef4444)' }} />
          <span>Air quality (AQI)</span>
        </label>
        {status && <span className="ml-auto text-gray-600 text-xs">{status}</span>}
      </div>
      <div className="relative flex-1 min-h-0">
        {/* maplibre-gl.css forces position:relative on the map element, so size it via a wrapper */}
        <div className="absolute inset-0"><div ref={mapContainer} className="w-full h-full" /></div>
        {selected && (
          <aside className="absolute top-3 left-3 bottom-3 w-80 max-w-[calc(100%-1.5rem)] bg-white rounded-xl shadow-xl border border-gray-200 flex flex-col z-10">
            <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100">
              <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                {{ vector: 'OSM feature', imagery: 'Sentinel-2 scene', air: 'Air quality station' }[selected.kind]}
              </span>
              <button onClick={() => setSelected(null)} aria-label="Close details" className="text-gray-400 hover:text-gray-900 text-xl leading-none">×</button>
            </div>
            <div className="flex-1 overflow-y-auto px-4 py-3">
              {selected.kind === 'vector' && <VectorDetail f={selected.row} />}
              {selected.kind === 'air' && <AirDetail s={selected.row} />}
              {selected.kind === 'imagery' && <ImageryDetail scenes={selected.rows} index={selected.index} onPick={i => setSelected(s => ({ ...s, index: i }))} />}
            </div>
          </aside>
        )}
      </div>
    </div>
  )
}
