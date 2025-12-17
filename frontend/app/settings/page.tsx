'use client';

import { useRouter } from 'next/navigation';
import { useEffect } from 'react';
import { ProtectedRoute } from '@/components/auth/protected-route';

export default function SettingsPage() {
  const router = useRouter();

  useEffect(() => {
    // Redirect to profile settings by default
    router.replace('/settings/profile');
  }, [router]);

  return (
    <ProtectedRoute>
      <div className="min-h-screen flex items-center justify-center">
        <p className="text-gray-500">Redirecting...</p>
      </div>
    </ProtectedRoute>
  );
}
