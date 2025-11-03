import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { QueryProvider } from '@/lib/providers/query-provider'
import { FilterProvider } from '@/lib/contexts/filter-context'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Search Analytics Dashboard',
  description: 'Analyze customer search behavior and metrics',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <QueryProvider>
          <FilterProvider>
            {children}
          </FilterProvider>
        </QueryProvider>
      </body>
    </html>
  )
}
