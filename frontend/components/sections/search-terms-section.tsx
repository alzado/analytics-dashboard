'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchSearchTerms } from '@/lib/api'
import { useFilters } from '@/lib/contexts/filter-context'
import { Download, ArrowUpDown } from 'lucide-react'

export function SearchTermsSection() {
  const { filters } = useFilters()
  const [sortBy, setSortBy] = useState('queries')

  const { data: terms, isLoading, error } = useQuery({
    queryKey: ['search-terms', filters, sortBy],
    queryFn: () => fetchSearchTerms(filters, 100, sortBy),
  })

  const exportToCSV = () => {
    if (!terms) return

    const headers = ['Search Term', 'Queries', 'Purchases', 'Revenue', 'CTR', 'Conversion Rate', '# Words', '# Attributes']
    const rows = terms.map(t => [
      t.search_term,
      t.queries,
      t.purchases,
      t.revenue.toFixed(2),
      (t.ctr * 100).toFixed(2),
      (t.conversion_rate * 100).toFixed(2),
      t.n_words,
      t.n_attributes
    ])

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.join(','))
    ].join('\n')

    const blob = new Blob([csvContent], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `search-terms-${new Date().toISOString().split('T')[0]}.csv`
    a.click()
  }

  if (isLoading) {
    return <div className="flex items-center justify-center h-64">Loading...</div>
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading search terms
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-lg font-semibold text-gray-900">Top Search Terms</h2>
        <button
          onClick={exportToCSV}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          <Download size={16} />
          Export CSV
        </button>
      </div>

      <div className="bg-white shadow overflow-hidden rounded-lg">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Search Term
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => setSortBy('queries')}
              >
                <div className="flex items-center gap-1">
                  Queries
                  <ArrowUpDown size={14} />
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => setSortBy('purchases')}
              >
                <div className="flex items-center gap-1">
                  Purchases
                  <ArrowUpDown size={14} />
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => setSortBy('revenue')}
              >
                <div className="flex items-center gap-1">
                  Revenue
                  <ArrowUpDown size={14} />
                </div>
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                CTR
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                CVR
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {terms?.map((term, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {term.search_term}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {term.queries.toLocaleString()}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {term.purchases.toLocaleString()}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  ${term.revenue.toFixed(2)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {(term.ctr * 100).toFixed(2)}%
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {(term.conversion_rate * 100).toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
