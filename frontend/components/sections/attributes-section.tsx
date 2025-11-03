'use client'

import { useQuery } from '@tanstack/react-query'
import { fetchDimensionBreakdown } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend } from 'recharts'

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#14b8a6', '#f97316']

export function AttributesSection() {
  const { filters } = useFilters()

  const { data: nAttributesData, isLoading: loadingNAttributes } = useQuery({
    queryKey: ['n-attributes-breakdown', filters],
    queryFn: () => fetchDimensionBreakdown('n_attributes', filters, 10),
  })

  const { data: nWordsData, isLoading: loadingNWords } = useQuery({
    queryKey: ['n-words-breakdown', filters],
    queryFn: () => fetchDimensionBreakdown('n_words', filters, 10),
  })

  if (loadingNAttributes || loadingNWords) {
    return <div className="flex items-center justify-center h-64">Loading...</div>
  }

  return (
    <div className="space-y-8">
      {/* Number of Attributes Section */}
      <div>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Performance by Number of Attributes</h2>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Distribution */}
          <div className="bg-white p-6 rounded-lg shadow">
            <h3 className="text-md font-semibold mb-4">Query Distribution</h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={nAttributesData}
                  dataKey="percentage_of_total"
                  nameKey="dimension_value"
                  cx="50%"
                  cy="50%"
                  outerRadius={80}
                  label={(entry) => `${entry.dimension_value} attrs: ${entry.percentage_of_total.toFixed(1)}%`}
                >
                  {nAttributesData?.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value: number) => `${value.toFixed(2)}%`} />
              </PieChart>
            </ResponsiveContainer>
          </div>

          {/* Conversion Rate */}
          <div className="bg-white p-6 rounded-lg shadow">
            <h3 className="text-md font-semibold mb-4">Conversion Rate by # Attributes</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={nAttributesData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="dimension_value" label={{ value: '# Attributes', position: 'insideBottom', offset: -5 }} />
                <YAxis />
                <Tooltip formatter={(value: number) => `${(value * 100).toFixed(2)}%`} />
                <Bar dataKey="conversion_rate" fill="#10b981" name="Conversion Rate" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Revenue by Attributes */}
        <div className="bg-white p-6 rounded-lg shadow mt-6">
          <h3 className="text-md font-semibold mb-4">Revenue by Number of Attributes</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={nAttributesData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="dimension_value" label={{ value: '# Attributes', position: 'insideBottom', offset: -5 }} />
              <YAxis />
              <Tooltip formatter={(value: number) => `$${value.toFixed(2)}`} />
              <Bar dataKey="revenue" fill="#8b5cf6" name="Revenue" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Number of Words Section */}
      <div>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Performance by Query Length (# Words)</h2>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Query Volume */}
          <div className="bg-white p-6 rounded-lg shadow">
            <h3 className="text-md font-semibold mb-4">Queries by Word Count</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={nWordsData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="dimension_value" label={{ value: '# Words', position: 'insideBottom', offset: -5 }} />
                <YAxis />
                <Tooltip />
                <Bar dataKey="queries" fill="#3b82f6" name="Queries" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* CTR by Words */}
          <div className="bg-white p-6 rounded-lg shadow">
            <h3 className="text-md font-semibold mb-4">CTR by Word Count</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={nWordsData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="dimension_value" label={{ value: '# Words', position: 'insideBottom', offset: -5 }} />
                <YAxis />
                <Tooltip formatter={(value: number) => `${(value * 100).toFixed(2)}%`} />
                <Bar dataKey="ctr" fill="#f59e0b" name="CTR" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Conversion & Revenue */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
          <div className="bg-white p-6 rounded-lg shadow">
            <h3 className="text-md font-semibold mb-4">Conversion Rate by Word Count</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={nWordsData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="dimension_value" label={{ value: '# Words', position: 'insideBottom', offset: -5 }} />
                <YAxis />
                <Tooltip formatter={(value: number) => `${(value * 100).toFixed(2)}%`} />
                <Bar dataKey="conversion_rate" fill="#10b981" name="Conversion Rate" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-white p-6 rounded-lg shadow">
            <h3 className="text-md font-semibold mb-4">Revenue per Query by Word Count</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={nWordsData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="dimension_value" label={{ value: '# Words', position: 'insideBottom', offset: -5 }} />
                <YAxis />
                <Tooltip formatter={(value: number) => `$${value.toFixed(2)}`} />
                <Bar dataKey="revenue_per_query" fill="#ec4899" name="Revenue/Query" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  )
}
