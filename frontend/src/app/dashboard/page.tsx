'use client'

import { useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { useRouter } from 'next/navigation'
import { isAuthenticated } from '@/lib/auth'
import TopBar from '@/components/layout/TopBar'
import TabNav from '@/components/layout/TabNav'
import StylePanel from '@/components/style-panel/StylePanel'
import AccordionSection from '@/components/layout/AccordionSection'
import { TabGeografico } from '@/types'

const KpiCards = dynamic(() => import('@/components/charts/KpiCards'), { ssr: false })
const PoblacionNacionalChart = dynamic(() => import('@/components/charts/PoblacionNacionalChart'), { ssr: false })
const GruposEdadNacionalChart = dynamic(() => import('@/components/charts/GruposEdadNacionalChart'), { ssr: false })
const PoblacionSexoNacionalChart = dynamic(() => import('@/components/charts/PoblacionSexoNacionalChart'), { ssr: false })
const PEANacionalChart = dynamic(() => import('@/components/charts/PEANacionalChart'), { ssr: false })
const OcupacionSectorChart = dynamic(() => import('@/components/charts/OcupacionSectorChart'), { ssr: false })
const PIBAnualChart = dynamic(() => import('@/components/charts/PIBAnualChart'), { ssr: false })
const PIBProyeccionChart = dynamic(() => import('@/components/charts/PIBProyeccionChart'), { ssr: false })
const InflacionNacionalChart = dynamic(() => import('@/components/charts/InflacionNacionalChart'), { ssr: false })
const TipoCambioChart = dynamic(() => import('@/components/charts/TipoCambioChart'), { ssr: false })
const IEDSectorChart = dynamic(() => import('@/components/charts/IEDSectorChart'), { ssr: false })
const IEDPaisChart = dynamic(() => import('@/components/charts/IEDPaisChart'), { ssr: false })
const AnunciosInversionChart = dynamic(() => import('@/components/charts/AnunciosInversionChart'), { ssr: false })
const AnunciosBaseChart = dynamic(() => import('@/components/charts/AnunciosBaseChart'), { ssr: false })
const PIBSectorChart = dynamic(() => import('@/components/charts/PIBSectorChart'), { ssr: false })
const BalanzaComercialChart = dynamic(() => import('@/components/charts/BalanzaComercialChart'), { ssr: false })
const IEDEstadosChart = dynamic(() => import('@/components/charts/IEDEstadosChart'), { ssr: false })
const EstadosPoblacionPIB = dynamic(() => import('@/components/charts/estatal/EstadosPoblacionPIB'), { ssr: false })
const DemografiaEstatalChart = dynamic(() => import('@/components/charts/estatal/DemografiaEstatalChart'), { ssr: false })
const ProyeccionesEstatalChart = dynamic(() => import('@/components/charts/estatal/ProyeccionesEstatalChart'), { ssr: false })
const ITAEEEstatalChart = dynamic(() => import('@/components/charts/estatal/ITAEEEstatalChart'), { ssr: false })
const AnunciosEstatalChart = dynamic(() => import('@/components/charts/estatal/AnunciosEstatalChart'), { ssr: false })
const HoteleraEstatalChart = dynamic(() => import('@/components/charts/estatal/HoteleraEstatalChart'), { ssr: false })
const LlegadaTuristasEstatalChart = dynamic(() => import('@/components/charts/estatal/LlegadaTuristasEstatalChart'), { ssr: false })
const ExportacionesEstatalChart = dynamic(() => import('@/components/charts/estatal/ExportacionesEstatalChart'), { ssr: false })
const AeropuertosEstatalChart = dynamic(() => import('@/components/charts/estatal/AeropuertosEstatalChart'), { ssr: false })
const RedCarreteraEstatalChart = dynamic(() => import('@/components/charts/estatal/RedCarreteraEstatalChart'), { ssr: false })
const MunicipiosPoblacionKpis = dynamic(() => import('@/components/charts/municipal/MunicipiosPoblacionKpis'), { ssr: false })
const LocalidadesPoblacionKpis = dynamic(() => import('@/components/charts/localidad/LocalidadesPoblacionKpis'), { ssr: false })
const CiudadesIndicadoresPanel = dynamic(() => import('@/components/charts/ciudad/CiudadesIndicadoresPanel'), { ssr: false })
const MapaInteractivoPanel = dynamic(
  () => import('@/components/charts/mapa/MapaInteractivoPanel'),
  { ssr: false }
)
const TurismoIngresosChart = dynamic(() => import('@/components/charts/TurismoIngresosChart'), { ssr: false })
const TurismoRankingChart = dynamic(() => import('@/components/charts/TurismoRankingChart'), { ssr: false })
const BalanzaVisitantesChart = dynamic(() => import('@/components/charts/BalanzaVisitantesChart'), { ssr: false })
const ActividadHoteleraChart = dynamic(() => import('@/components/charts/ActividadHoteleraChart'), { ssr: false })
const MercadoAereoChart = dynamic(() => import('@/components/charts/MercadoAereoChart'), { ssr: false })
const OperacionesAeroportuariasChart = dynamic(() => import('@/components/charts/OperacionesAeroportuariasChart'), { ssr: false })

export default function DashboardPage() {
  const router = useRouter()
  const [activeTab, setActiveTab] = useState<TabGeografico>('nacional')
  const [mounted, setMounted] = useState(false)
  const [selectedEstado, setSelectedEstado] = useState('Jalisco')

  useEffect(() => {
    setMounted(true)
    if (!isAuthenticated()) {
      router.replace('/login')
    }
  }, [router])

  if (!mounted) return null

  return (
    <div
      style={{
        minHeight: '100vh',
        background: '#0f1117',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      {/* Top bar: logo + title + logout */}
      <TopBar />

      {/* Style panel: typography + color palette controls */}
      <StylePanel />

      {/* Geographic tab navigation */}
      <TabNav activeTab={activeTab} onTabChange={setActiveTab} />

      {/* Charts area */}
      <main
        style={{
          flex: 1,
          padding: '24px',
          display: 'flex',
          flexDirection: 'column',
          gap: '24px',
        }}
      >
        {activeTab === 'nacional' && (
          <>
            <AccordionSection title="KPIs Nacionales" accentColor="#0576F3" defaultOpen={true}>
              <KpiCards />
            </AccordionSection>

            <AccordionSection title="Demografía" accentColor="#36F48C" defaultOpen={true}>
              <PoblacionNacionalChart />
              <GruposEdadNacionalChart />
              <PoblacionSexoNacionalChart />
            </AccordionSection>

            <AccordionSection title="Economía" accentColor="#F47806" defaultOpen={true}>
              <PEANacionalChart />
              <OcupacionSectorChart />
              <PIBAnualChart />
              <PIBProyeccionChart />
              <InflacionNacionalChart />
              <TipoCambioChart />
              <IEDSectorChart />
              <IEDPaisChart />
              <AnunciosInversionChart />
              <AnunciosBaseChart />
              <PIBSectorChart />
              <BalanzaComercialChart />
              <IEDEstadosChart />
            </AccordionSection>

            <AccordionSection title="Turismo" accentColor="#F3F40B" defaultOpen={true}>
              <TurismoRankingChart />
              <TurismoIngresosChart />
              <BalanzaVisitantesChart />
              <ActividadHoteleraChart />
            </AccordionSection>

            <AccordionSection title="Conectividad Aérea" accentColor="#F479F4" defaultOpen={true}>
              <MercadoAereoChart />
              <OperacionesAeroportuariasChart />
            </AccordionSection>
          </>
        )}

        {activeTab === 'estatal' && (
          <>
            <EstadosPoblacionPIB estado={selectedEstado} onEstadoChange={setSelectedEstado} />
            <DemografiaEstatalChart estado={selectedEstado} />
            <ProyeccionesEstatalChart estado={selectedEstado} />
            <ITAEEEstatalChart estado={selectedEstado} />
            <AnunciosEstatalChart estado={selectedEstado} />
            <HoteleraEstatalChart estado={selectedEstado} />
            <LlegadaTuristasEstatalChart estado={selectedEstado} />
            <ExportacionesEstatalChart estado={selectedEstado} />
            <AeropuertosEstatalChart estado={selectedEstado} />
            <RedCarreteraEstatalChart estado={selectedEstado} />
          </>
        )}

        {activeTab === 'municipal' && (
          <>
            <MunicipiosPoblacionKpis />
          </>
        )}

        {activeTab === 'ciudad' && (
          <>
            <CiudadesIndicadoresPanel />
          </>
        )}

        {activeTab === 'localidad' && (
          <>
            <LocalidadesPoblacionKpis />
          </>
        )}

        {activeTab === 'mapa' && (
          <>
            <MapaInteractivoPanel />
          </>
        )}

        {activeTab !== 'nacional' &&
          activeTab !== 'estatal' &&
          activeTab !== 'municipal' &&
          activeTab !== 'ciudad' &&
          activeTab !== 'localidad' &&
          activeTab !== 'mapa' && (
          <div
            style={{
              textAlign: 'center',
              color: '#4a5568',
              padding: '64px 0',
              fontSize: '14px',
            }}
          >
            Nivel geográfico: <strong style={{ color: '#94a3b8' }}>{activeTab}</strong>
            <br />
            <span style={{ fontSize: '12px', marginTop: '8px', display: 'block' }}>
              Los indicadores se mostrarán aquí conforme se incorporen al sistema.
            </span>
          </div>
        )}
      </main>
    </div>
  )
}
