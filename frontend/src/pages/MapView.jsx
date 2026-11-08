import { useEffect, useRef, useState } from 'react'
import * as maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { api } from '../api/client.js'

const KERALA_CENTER = [76.3, 10.5]
const KERALA_ZOOM = 7

export default function MapView() {
  const mapContainer = useRef(null)
  const map = useRef(null)
  const [layers, setLayers] = useState({ vector: true, imagery: false })
  const [status, setStatus] = useState('')

  useEffect(() => {
    if (map.current) return
    map.current = new maplibregl.Map({
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

    map.current.addControl(new maplibregl.NavigationControl(), 'top-right')

    map.current.on('load', async () => {
      await loadVectorLayer()
    })
  }, [])

  async function loadVectorLayer() {
    if (!map.current) return
    setStatus('Loading vector features…')
    try {
      const data = await api.get('/vector/features/?limit=500')
      const geojson = {
        type: 'FeatureCollection',
        features: Array.isArray(data) ? data.map(f => ({
          type: 'Feature',
          geometry: f.geometry || { type: 'Point', coordinates: [f.longitude || 76.3, f.latitude || 10.5] },
          properties: f,
        })) : [],
      }
      if (map.current.getSource('vector-features')) {
        map.current.getSource('vector-features').setData(geojson)
      } else {
        map.current.addSource('vector-features', { type: 'geojson', data: geojson })
        map.current.addLayer({ id: 'vector-points', type: 'circle', source: 'vector-features',
          filter: ['==', ['geometry-type'], 'Point'],
          paint: { 'circle-radius': 5, 'circle-color': '#3b82f6', 'circle-opacity': 0.8 } })
        map.current.addLayer({ id: 'vector-lines', type: 'line', source: 'vector-features',
          filter: ['in', ['geometry-type'], ['literal', ['LineString', 'MultiLineString']]],
          paint: { 'line-color': '#10b981', 'line-width': 2 } })
      }
      setStatus(`${geojson.features.length} features loaded`)
    } catch {
      setStatus('Vector features unavailable')
    }
  }

  async function loadImageryLayer() {
    if (!map.current) return
    setStatus('Loading imagery footprints…')
    try {
      const data = await api.get('/imagery/?limit=200')
      const geojson = {
        type: 'FeatureCollection',
        features: Array.isArray(data) ? data.map(f => ({
          type: 'Feature',
          geometry: f.footprint || null,
          properties: f,
        })).filter(f => f.geometry) : [],
      }
      if (map.current.getSource('imagery')) {
        map.current.getSource('imagery').setData(geojson)
      } else {
        map.current.addSource('imagery', { type: 'geojson', data: geojson })
        map.current.addLayer({ id: 'imagery-fills', type: 'fill', source: 'imagery',
          paint: { 'fill-color': '#f59e0b', 'fill-opacity': 0.2 } })
        map.current.addLayer({ id: 'imagery-outlines', type: 'line', source: 'imagery',
          paint: { 'line-color': '#f59e0b', 'line-width': 1 } })
      }
      setStatus(`${geojson.features.length} imagery footprints loaded`)
    } catch {
      setStatus('Imagery unavailable')
    }
  }

  function toggleLayer(name) {
    const next = !layers[name]
    setLayers(l => ({ ...l, [name]: next }))
    if (!map.current) return
    if (name === 'vector') {
      const vis = next ? 'visible' : 'none'
      if (map.current.getLayer('vector-points')) map.current.setLayoutProperty('vector-points', 'visibility', vis)
      if (map.current.getLayer('vector-lines')) map.current.setLayoutProperty('vector-lines', 'visibility', vis)
      if (next) loadVectorLayer()
    }
    if (name === 'imagery') {
      const vis = next ? 'visible' : 'none'
      if (map.current.getLayer('imagery-fills')) { map.current.setLayoutProperty('imagery-fills', 'visibility', vis); map.current.setLayoutProperty('imagery-outlines', 'visibility', vis) }
      if (next) loadImageryLayer()
    }
  }

  return (
    <div className="h-screen flex flex-col bg-gray-950">
      <div className="flex items-center gap-4 px-4 py-2 bg-gray-900 border-b border-gray-800 text-sm text-white">
        <span className="font-semibold">Map View</span>
        <label className="flex items-center gap-1 cursor-pointer">
          <input type="checkbox" checked={layers.vector} onChange={() => toggleLayer('vector')} />
          <span className="text-blue-300">Vector features</span>
        </label>
        <label className="flex items-center gap-1 cursor-pointer">
          <input type="checkbox" checked={layers.imagery} onChange={() => toggleLayer('imagery')} />
          <span className="text-amber-300">Satellite footprints</span>
        </label>
        {status && <span className="ml-auto text-gray-400 text-xs">{status}</span>}
      </div>
      <div ref={mapContainer} className="flex-1" />
    </div>
  )
}
