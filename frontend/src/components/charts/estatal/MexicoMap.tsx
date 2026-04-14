'use client'

import { useEffect, useState } from 'react'

const GEO_URL = 'https://raw.githubusercontent.com/angelnmara/geojson/master/mexicoHigh.json'

const NAME_MAP: Record<string, string> = {
  'Distrito Federal': 'Ciudad de México',
  'México': 'Estado de México',
  'Coahuila de Zaragoza': 'Coahuila',
  'Michoacán de Ocampo': 'Michoacán',
  'Veracruz de Ignacio de la Llave': 'Veracruz',
}

function normalizeName(n: string): string {
  return NAME_MAP[n] ?? n
}

// Simple Mercator projection for Mexico
function project(lon: number, lat: number): [number, number] {
  const cx = -102, cy = 23.5, scale = 18
  const x = (lon - cx) * scale + 350
  const y = -(lat - cy) * scale + 225
  return [x, y]
}

function coordsToPath(coords: number[][][]): string {
  return coords.map((ring) => {
    const pts = ring.map(([lon, lat]) => {
      const [x, y] = project(lon, lat)
      return `${x},${y}`
    })
    return `M${pts.join('L')}Z`
  }).join(' ')
}

interface Feature {
  properties: { name?: string; NAME?: string }
  geometry: { type: string; coordinates: number[][][][] | number[][][] }
}

interface MexicoMapProps {
  selected: string
  onSelect: (estado: string) => void
  highlightColor: string
  defaultColor: string
}

export default function MexicoMap({ selected, onSelect, highlightColor, defaultColor }: MexicoMapProps) {
  const [features, setFeatures] = useState<Feature[]>([])
  const [hovered, setHovered] = useState<string | null>(null)

  useEffect(() => {
    fetch(GEO_URL)
      .then((r) => r.json())
      .then((data) => {
        const feats = data.features ?? data.objects?.MEX?.geometries ?? []
        setFeatures(feats)
      })
      .catch(() => {})
  }, [])

  if (!features.length) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '300px', color: '#4a5568', fontSize: '13px' }}>
        Cargando mapa...
      </div>
    )
  }

  return (
    <svg viewBox="0 0 700 450" width="100%" style={{ maxHeight: '450px' }}>
      <rect width="700" height="450" fill="transparent" />
      {features.map((feat, i) => {
        const name = normalizeName(feat.properties?.name ?? feat.properties?.NAME ?? `state-${i}`)
        const isSelected = name === selected
        const isHovered = name === hovered
        const geom = feat.geometry

        let paths: string[] = []
        if (geom.type === 'Polygon') {
          paths = [coordsToPath(geom.coordinates as number[][][])]
        } else if (geom.type === 'MultiPolygon') {
          paths = (geom.coordinates as number[][][][]).map((poly) => coordsToPath(poly))
        }

        return (
          <g key={i} onClick={() => onSelect(name)}
            onMouseEnter={() => setHovered(name)}
            onMouseLeave={() => setHovered(null)}
            style={{ cursor: 'pointer' }}>
            {paths.map((d, j) => (
              <path key={j} d={d}
                fill={isSelected ? highlightColor : isHovered ? '#3a3f5c' : defaultColor}
                stroke={isSelected ? highlightColor : '#2d3148'}
                strokeWidth={isSelected ? 1.5 : 0.5}
              />
            ))}
          </g>
        )
      })}
    </svg>
  )
}
