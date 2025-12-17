'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { useAuth } from '@/lib/contexts/auth-context';

export default function GCPCallbackPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { completeBigQueryAuth, isAuthenticated, isLoading } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState(true);

  useEffect(() => {
    const handleCallback = async () => {
      // Wait for auth to initialize
      if (isLoading) return;

      // Must be authenticated to complete GCP OAuth
      if (!isAuthenticated) {
        router.push('/login?redirect=/auth/gcp-callback');
        return;
      }

      const code = searchParams.get('code');
      const errorParam = searchParams.get('error');

      if (errorParam) {
        setError(`Google OAuth error: ${errorParam}`);
        setIsProcessing(false);
        return;
      }

      if (!code) {
        setError('No authorization code received');
        setIsProcessing(false);
        return;
      }

      try {
        await completeBigQueryAuth(code);
        // Redirect to credentials settings on success
        router.push('/settings/credentials?success=true&tab=gcp');
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to complete authorization');
        setIsProcessing(false);
      }
    };

    handleCallback();
  }, [searchParams, completeBigQueryAuth, isAuthenticated, isLoading, router]);

  if (isLoading || isProcessing) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Completing BigQuery authorization...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="max-w-md w-full bg-white shadow-lg rounded-lg p-6">
          <div className="text-center">
            <div className="mx-auto flex items-center justify-center h-12 w-12 rounded-full bg-red-100">
              <svg
                className="h-6 w-6 text-red-600"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M6 18L18 6M6 6l12 12"
                />
              </svg>
            </div>
            <h3 className="mt-4 text-lg font-medium text-gray-900">
              Authorization Failed
            </h3>
            <p className="mt-2 text-sm text-gray-500">{error}</p>
            <div className="mt-6 flex justify-center space-x-3">
              <button
                onClick={() => router.push('/settings/credentials')}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-md"
              >
                Back to Settings
              </button>
              <button
                onClick={() => window.location.reload()}
                className="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-md"
              >
                Try Again
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return null;
}
