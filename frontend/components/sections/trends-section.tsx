'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchTrendData } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { format, parseISO } from 'date-fns'

export function TrendsSection() {
  const { filters } = useFilters()

  const { data: trends, isLoading, error } = useQuery({
    queryKey: ['trends', filters],
    queryFn: () => fetchTrendData(filters, 'daily'),
  })

  if (isLoading) {
    return <div className="flex items-center justify-center h-64">Loading...</div>
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading trend data
      </div>
    )
  }

  if (!trends || trends.length === 0) {
    return <div>No data available</div>
  }

  const formattedData = trends.map(d => ({
    ...d,
    displayDate: format(parseISO(d.date), 'MMM d'),
  }))

  return (
    <div className="space-y-6">
      <h2 className="text-lg font-semibold text-gray-900">Query Trends</h2>

      <div className="bg-white p-6 rounded-lg shadow">
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={formattedData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="displayDate" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="queries" stroke="#3b82f6" name="Queries" strokeWidth={2} />
            <Line type="monotone" dataKey="purchases" stroke="#10b981" name="Purchases" strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <h2 className="text-lg font-semibold text-gray-900 mt-8">Conversion Rate Trends</h2>

      <div className="bg-white p-6 rounded-lg shadow">
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={formattedData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="displayDate" />
            <YAxis />
            <Tooltip formatter={(value: number) => `${(value * 100).toFixed(2)}%`} />
            <Legend />
            <Line type="monotone" dataKey="ctr" stroke="#8b5cf6" name="CTR" strokeWidth={2} />
            <Line type="monotone" dataKey="conversion_rate" stroke="#f59e0b" name="Conversion Rate" strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
