'use client';

import { useCallback, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { GoogleLogin, CredentialResponse } from '@react-oauth/google';
import { useAuth } from '@/lib/contexts/auth-context';
import { BarChart3, AlertCircle, Loader2 } from 'lucide-react';

export function LoginPage() {
  const router = useRouter();
  const { login, isAuthenticated, isLoading, error: authError } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [isLoggingIn, setIsLoggingIn] = useState(false);

  // Redirect if already authenticated
  useEffect(() => {
    if (isAuthenticated && !isLoading) {
      router.push('/');
    }
  }, [isAuthenticated, isLoading, router]);

  const handleGoogleSuccess = useCallback(async (credentialResponse: CredentialResponse) => {
    if (!credentialResponse.credential) {
      setError('No credential received from Google');
      return;
    }

    setIsLoggingIn(true);
    setError(null);

    try {
      await login(credentialResponse.credential);
      router.push('/');
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Login failed';
      setError(message);
    } finally {
      setIsLoggingIn(false);
    }
  }, [login, router]);

  const handleGoogleError = useCallback(() => {
    setError('Google sign-in failed. Please try again.');
  }, []);

  // Show loading while checking auth state
  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="flex items-center gap-2 text-gray-600">
          <Loader2 className="w-5 h-5 animate-spin" />
          <span>Loading...</span>
        </div>
      </div>
    );
  }

  // Don't show login page if already authenticated
  if (isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-gray-50 px-4">
      <div className="w-full max-w-md">
        {/* Logo / Branding */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 bg-blue-600 rounded-xl mb-4">
            <BarChart3 className="w-8 h-8 text-white" />
          </div>
          <h1 className="text-2xl font-bold text-gray-900">Search Analytics</h1>
          <p className="mt-2 text-gray-600">
            Sign in to access your search analytics dashboard
          </p>
        </div>

        {/* Login Card */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8">
          <h2 className="text-lg font-semibold text-gray-900 text-center mb-6">
            Sign in to your account
          </h2>

          {/* Error Message */}
          {(error || authError) && (
            <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-red-600 shrink-0 mt-0.5" />
              <div className="text-sm text-red-700">
                {error || authError}
              </div>
            </div>
          )}

          {/* Google Login Button */}
          <div className="flex justify-center">
            {isLoggingIn ? (
              <div className="flex items-center gap-2 py-3 px-6 text-gray-600">
                <Loader2 className="w-5 h-5 animate-spin" />
                <span>Signing in...</span>
              </div>
            ) : (
              <GoogleLogin
                onSuccess={handleGoogleSuccess}
                onError={handleGoogleError}
                theme="outline"
                size="large"
                width="300"
                text="signin_with"
                shape="rectangular"
              />
            )}
          </div>

          {/* Additional info */}
          <p className="mt-6 text-xs text-center text-gray-500">
            By signing in, you agree to our Terms of Service and Privacy Policy.
          </p>
        </div>

        {/* Footer */}
        <div className="mt-8 text-center text-sm text-gray-500">
          <p>
            Need help?{' '}
            <a href="mailto:support@example.com" className="text-blue-600 hover:underline">
              Contact support
            </a>
          </p>
        </div>
      </div>
    </div>
  );
}
