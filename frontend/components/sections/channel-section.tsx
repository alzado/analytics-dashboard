'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchDimensionBreakdown } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'

export function ChannelSection() {
  const { filters } = useFilters()

  const { data: channelData, isLoading, error } = useQuery({
    queryKey: ['channel-breakdown', filters],
    queryFn: () => fetchDimensionBreakdown('channel', filters, 10),
  })

  if (isLoading) {
    return <div className="flex items-center justify-center h-64">Loading...</div>
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading channel data
      </div>
    )
  }

  if (!channelData || channelData.length === 0) {
    return <div>No channel data available</div>
  }

  // Format data for charts
  const chartData = channelData.map(d => ({
    ...d,
    ctr_pct: d.ctr * 100,
    conversion_pct: d.conversion_rate * 100,
    a2c_pct: d.a2c_rate * 100,
  }))

  return (
    <div className="space-y-6">
      <h2 className="text-lg font-semibold text-gray-900">Channel Performance</h2>

      {/* Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {channelData.map((channel) => (
          <div key={channel.dimension_value} className="bg-white p-6 rounded-lg shadow border-l-4 border-blue-500">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">{channel.dimension_value}</h3>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-600">Queries:</span>
                <span className="font-semibold">{channel.queries.toLocaleString()}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Revenue:</span>
                <span className="font-semibold">${channel.revenue.toFixed(2)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">CTR:</span>
                <span className="font-semibold">{(channel.ctr * 100).toFixed(2)}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Conversion:</span>
                <span className="font-semibold">{(channel.conversion_rate * 100).toFixed(2)}%</span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* CTR Comparison */}
      <div className="bg-white p-6 rounded-lg shadow">
        <h3 className="text-md font-semibold mb-4">Click-Through Rate by Channel</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="dimension_value" />
            <YAxis />
            <Tooltip formatter={(value: number) => `${value.toFixed(2)}%`} />
            <Bar dataKey="ctr_pct" fill="#3b82f6" name="CTR %" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Conversion Rate Comparison */}
      <div className="bg-white p-6 rounded-lg shadow">
        <h3 className="text-md font-semibold mb-4">Conversion Rate by Channel</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="dimension_value" />
            <YAxis />
            <Tooltip formatter={(value: number) => `${value.toFixed(2)}%`} />
            <Bar dataKey="conversion_pct" fill="#10b981" name="Conversion %" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Revenue Comparison */}
      <div className="bg-white p-6 rounded-lg shadow">
        <h3 className="text-md font-semibold mb-4">Revenue by Channel</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="dimension_value" />
            <YAxis />
            <Tooltip formatter={(value: number) => `$${value.toFixed(2)}`} />
            <Bar dataKey="revenue" fill="#8b5cf6" name="Revenue" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
