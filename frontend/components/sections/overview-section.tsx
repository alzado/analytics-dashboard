'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchOverviewMetrics } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import { KPICard } from '../ui/kpi-card'

export function OverviewSection() {
  const { filters } = useFilters()

  const { data: metrics, isLoading, error } = useQuery({
    queryKey: ['overview', filters],
    queryFn: () => fetchOverviewMetrics(filters),
  })

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading metrics. Please check your backend connection.
      </div>
    )
  }

  if (!metrics) return null

  return (
    <div className="space-y-6">
      <h2 className="text-lg font-semibold text-gray-900">Key Metrics</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          title="Total Queries"
          value={metrics.queries.toLocaleString()}
          trend="+5.2%"
        />
        <KPICard
          title="Total Revenue"
          value={`$${metrics.revenue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
          trend="+12.3%"
        />
        <KPICard
          title="Conversion Rate"
          value={`${(metrics.conversion_rate * 100).toFixed(2)}%`}
          trend="+0.8%"
        />
        <KPICard
          title="CTR"
          value={`${(metrics.ctr * 100).toFixed(2)}%`}
          trend="-1.2%"
          trendColor="red"
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-6">
        <KPICard
          title="Unique Search Terms"
          value={metrics.unique_search_terms.toLocaleString()}
        />
        <KPICard
          title="Revenue per Query"
          value={`$${metrics.revenue_per_query.toFixed(2)}`}
        />
        <KPICard
          title="Average Order Value"
          value={`$${metrics.aov.toFixed(2)}`}
        />
      </div>
    </div>
  )
}
