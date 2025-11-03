interface KPICardProps {
  title: string
  value: string
  trend?: string
  trendColor?: 'green' | 'red'
}

export function KPICard({ title, value, trend, trendColor = 'green' }: KPICardProps) {
  return (
    <div className="bg-white rounded-lg shadow p-6 border-l-4 border-blue-500">
      <h3 className="text-sm font-medium text-gray-500 mb-2">{title}</h3>
      <p className="text-3xl font-bold text-gray-900 mb-2">{value}</p>
      {trend && (
        <p className={`text-sm ${trendColor === 'green' ? 'text-green-600' : 'text-red-600'}`}>
          {trend}
        </p>
      )}
    </div>
  )
}
