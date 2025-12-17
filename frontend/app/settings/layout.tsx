'use client';

import { ReactNode } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ProtectedRoute } from '@/components/auth/protected-route';
import { User, Building2, Database, ArrowLeft } from 'lucide-react';

const settingsNav = [
  { href: '/settings/profile', label: 'Profile', icon: User },
  { href: '/settings/organizations', label: 'Organizations', icon: Building2 },
  { href: '/settings/credentials', label: 'BigQuery Access', icon: Database },
];

export default function SettingsLayout({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <ProtectedRoute>
      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <header className="bg-white border-b border-gray-200">
          <div className="max-w-6xl mx-auto px-4 py-4">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors"
              >
                <ArrowLeft className="w-5 h-5" />
                <span>Back to Dashboard</span>
              </Link>
              <div className="h-6 w-px bg-gray-300" />
              <h1 className="text-xl font-semibold text-gray-900">Settings</h1>
            </div>
          </div>
        </header>

        <div className="max-w-6xl mx-auto px-4 py-8">
          <div className="flex gap-8">
            {/* Sidebar Navigation */}
            <nav className="w-64 flex-shrink-0">
              <ul className="space-y-1">
                {settingsNav.map((item) => {
                  const isActive = pathname === item.href;
                  const Icon = item.icon;
                  return (
                    <li key={item.href}>
                      <Link
                        href={item.href}
                        className={`flex items-center gap-3 px-4 py-2.5 rounded-lg transition-colors ${
                          isActive
                            ? 'bg-blue-50 text-blue-700 font-medium'
                            : 'text-gray-700 hover:bg-gray-100'
                        }`}
                      >
                        <Icon className="w-5 h-5" />
                        {item.label}
                      </Link>
                    </li>
                  );
                })}
              </ul>
            </nav>

            {/* Main Content */}
            <main className="flex-1 min-w-0">
              {children}
            </main>
          </div>
        </div>
      </div>
    </ProtectedRoute>
  );
}
