'use client'

import { useEffect } from 'react'
import { type WidgetConfig } from '@/lib/api'
import { usePivotConfig } from '@/hooks/use-pivot-config'
import { usePivotFilters } from '@/hooks/use-pivot-filters'
import { PivotTableSection } from '@/components/sections/pivot-table-section'

interface MultiTableWidgetProps {
  widget: WidgetConfig
}

/**
 * MultiTableWidget - Wrapper that renders PivotTableSection with widget configuration
 * This ensures the widget always matches the Editor's appearance and behavior
 */
export function MultiTableWidget({ widget }: MultiTableWidgetProps) {
  return (
    <div className="h-full w-full">
      <PivotTableSection
        widgetMode={true}
        widgetConfig={widget}
      />
    </div>
  )
}
