'use client';

import { ReactNode, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/lib/contexts/auth-context';
import { Loader2, AlertTriangle } from 'lucide-react';

interface ProtectedRouteProps {
  children: ReactNode;
  redirectTo?: string;
}

export function ProtectedRoute({
  children,
  redirectTo = '/login'
}: ProtectedRouteProps) {
  const router = useRouter();
  const { isAuthenticated, isLoading, isAuthEnabled } = useAuth();

  useEffect(() => {
    // Only redirect if auth is enabled and user is not authenticated
    if (isAuthEnabled && !isLoading && !isAuthenticated) {
      router.push(redirectTo);
    }
  }, [isAuthenticated, isLoading, isAuthEnabled, router, redirectTo]);

  // Show loading state (only when auth is enabled)
  if (isAuthEnabled && isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="flex flex-col items-center gap-3">
          <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
          <p className="text-sm text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  // If auth is not enabled (no Google Client ID), show configuration message
  if (!isAuthEnabled) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="max-w-md p-8 bg-white rounded-lg shadow-lg text-center">
          <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-yellow-100 flex items-center justify-center">
            <AlertTriangle className="w-8 h-8 text-yellow-600" />
          </div>
          <h1 className="text-xl font-semibold text-gray-900 mb-2">
            Authentication Not Configured
          </h1>
          <p className="text-gray-600 mb-4">
            Google OAuth is not configured. Please set the <code className="px-1 py-0.5 bg-gray-100 rounded text-sm">NEXT_PUBLIC_GOOGLE_CLIENT_ID</code> environment variable.
          </p>
          <div className="text-left bg-gray-50 rounded-lg p-4 text-sm">
            <p className="font-medium text-gray-700 mb-2">To configure:</p>
            <ol className="list-decimal list-inside space-y-1 text-gray-600">
              <li>Create OAuth credentials in Google Cloud Console</li>
              <li>Add <code className="px-1 py-0.5 bg-gray-100 rounded">NEXT_PUBLIC_GOOGLE_CLIENT_ID</code> to your environment</li>
              <li>Restart the application</li>
            </ol>
          </div>
        </div>
      </div>
    );
  }

  // Don't render children if not authenticated
  if (!isAuthenticated) {
    return null;
  }

  return <>{children}</>;
}
