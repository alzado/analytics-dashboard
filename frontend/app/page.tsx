'use client'

import { useState } from 'react'
import { DashboardLayout } from '@/components/layout/dashboard-layout'
import { PivotTableSection } from '@/components/sections/pivot-table-section'
import { UsageLogsSection } from '@/components/sections/usage-logs-section'
import { TablesSection } from '@/components/sections/tables-section'
import DashboardsSection from '@/components/sections/dashboards-section'
import { DashboardProvider } from '@/lib/contexts/dashboard-context'
import { WidgetEditingProvider } from '@/lib/contexts/widget-editing-context'

export default function Home() {
  const [activeTab, setActiveTab] = useState('tables')

  return (
    <WidgetEditingProvider>
      <DashboardProvider>
        <DashboardLayout activeTab={activeTab} onTabChange={setActiveTab}>
          {activeTab === 'dashboards' && <DashboardsSection onTabChange={setActiveTab} />}
          {activeTab === 'pivot' && <PivotTableSection onTabChange={setActiveTab} />}
          {activeTab === 'tables' && <TablesSection />}
          {activeTab === 'logs' && <UsageLogsSection />}
        </DashboardLayout>
      </DashboardProvider>
    </WidgetEditingProvider>
  )
}
