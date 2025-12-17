'use client';

import { useState, useEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import { useAuth } from '@/lib/contexts/auth-context';
import { Database, CheckCircle, XCircle, AlertCircle, RefreshCw, LogOut } from 'lucide-react';

export default function CredentialsSettingsPage() {
  const searchParams = useSearchParams();
  const { user, hasBigQueryAccess, authorizeBigQuery, revokeBigQueryAccess, isLoading } = useAuth();
  const [isAuthorizing, setIsAuthorizing] = useState(false);
  const [isRevoking, setIsRevoking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // Check for success parameter from OAuth callback
  useEffect(() => {
    const success = searchParams.get('success');
    const tab = searchParams.get('tab');

    if (success === 'true' && tab === 'gcp') {
      setSuccessMessage('BigQuery access authorized successfully!');
      // Clear message after 5 seconds
      const timer = setTimeout(() => setSuccessMessage(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [searchParams]);

  const handleAuthorize = async () => {
    setIsAuthorizing(true);
    setError(null);
    try {
      await authorizeBigQuery();
      // User will be redirected to Google OAuth
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start authorization');
      setIsAuthorizing(false);
    }
  };

  const handleRevoke = async () => {
    if (!confirm('Are you sure you want to revoke BigQuery access? You will need to re-authorize to query data.')) {
      return;
    }

    setIsRevoking(true);
    setError(null);
    try {
      await revokeBigQueryAccess();
      setSuccessMessage('BigQuery access revoked successfully');
      setTimeout(() => setSuccessMessage(null), 5000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to revoke access');
    } finally {
      setIsRevoking(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold text-gray-900">BigQuery Access</h2>
        <p className="text-gray-600 mt-1">
          Authorize access to Google BigQuery using your Google account
        </p>
      </div>

      {/* Success Message */}
      {successMessage && (
        <div className="flex items-center gap-3 p-4 bg-green-50 border border-green-200 rounded-lg">
          <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0" />
          <p className="text-green-800">{successMessage}</p>
        </div>
      )}

      {/* Error Message */}
      {error && (
        <div className="flex items-center gap-3 p-4 bg-red-50 border border-red-200 rounded-lg">
          <XCircle className="w-5 h-5 text-red-600 flex-shrink-0" />
          <p className="text-red-800">{error}</p>
        </div>
      )}

      {/* Current Status */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="font-medium text-gray-900 mb-4">Authorization Status</h3>

        {hasBigQueryAccess ? (
          <div className="space-y-4">
            <div className="flex items-center gap-3 p-4 bg-green-50 border border-green-200 rounded-lg">
              <CheckCircle className="w-6 h-6 text-green-600" />
              <div className="flex-1">
                <p className="font-medium text-green-800">BigQuery Access Authorized</p>
                <p className="text-sm text-green-600">
                  You can query BigQuery data using your Google account ({user?.email})
                </p>
              </div>
            </div>

            <div className="flex items-center gap-3">
              <button
                onClick={handleRevoke}
                disabled={isRevoking}
                className="flex items-center gap-2 px-4 py-2 text-red-600 bg-red-50 rounded-lg hover:bg-red-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isRevoking ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <LogOut className="w-4 h-4" />
                )}
                Revoke Access
              </button>
              <button
                onClick={handleAuthorize}
                disabled={isAuthorizing}
                className="flex items-center gap-2 px-4 py-2 text-blue-600 bg-blue-50 rounded-lg hover:bg-blue-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isAuthorizing ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <RefreshCw className="w-4 h-4" />
                )}
                Re-authorize
              </button>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="flex items-center gap-3 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
              <AlertCircle className="w-6 h-6 text-yellow-600" />
              <div className="flex-1">
                <p className="font-medium text-yellow-800">BigQuery Access Not Authorized</p>
                <p className="text-sm text-yellow-600">
                  You need to authorize BigQuery access to query data from the dashboard
                </p>
              </div>
            </div>

            <button
              onClick={handleAuthorize}
              disabled={isAuthorizing}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isAuthorizing ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Database className="w-4 h-4" />
              )}
              Authorize BigQuery Access
            </button>
          </div>
        )}
      </div>

      {/* Info Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="font-medium text-gray-900 mb-4">How it works</h3>
        <div className="space-y-4 text-sm text-gray-600">
          <div className="flex gap-3">
            <div className="flex-shrink-0 w-6 h-6 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center text-xs font-medium">
              1
            </div>
            <p>
              <strong className="text-gray-900">Sign in with Google</strong> - You&apos;ll be redirected to Google to sign in and grant BigQuery access permissions
            </p>
          </div>
          <div className="flex gap-3">
            <div className="flex-shrink-0 w-6 h-6 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center text-xs font-medium">
              2
            </div>
            <p>
              <strong className="text-gray-900">Access your data</strong> - Once authorized, you can query any BigQuery datasets and projects your Google account has access to
            </p>
          </div>
          <div className="flex gap-3">
            <div className="flex-shrink-0 w-6 h-6 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center text-xs font-medium">
              3
            </div>
            <p>
              <strong className="text-gray-900">Secure & private</strong> - Your credentials are stored securely and encrypted. Only you can access data with your permissions
            </p>
          </div>
        </div>
      </div>

      {/* Permissions Info */}
      <div className="bg-gray-50 rounded-lg border border-gray-200 p-6">
        <h3 className="font-medium text-gray-900 mb-3">Requested Permissions</h3>
        <ul className="space-y-2 text-sm text-gray-600">
          <li className="flex items-center gap-2">
            <CheckCircle className="w-4 h-4 text-green-500" />
            <span>View and manage your data in Google BigQuery</span>
          </li>
          <li className="flex items-center gap-2">
            <CheckCircle className="w-4 h-4 text-green-500" />
            <span>View your Google Cloud Platform project information</span>
          </li>
        </ul>
        <p className="mt-3 text-xs text-gray-500">
          These permissions are required to execute queries on your behalf. You can revoke access at any time.
        </p>
      </div>
    </div>
  );
}
