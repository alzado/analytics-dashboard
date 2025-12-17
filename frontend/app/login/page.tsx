'use client';

import { useEffect, useState } from 'react';
import type { ComponentType } from 'react';

// Loading component
function LoadingState() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="flex items-center gap-2 text-gray-600">
        <span>Loading...</span>
      </div>
    </div>
  );
}

export default function LoginPage() {
  const [LoginComponent, setLoginComponent] = useState<ComponentType | null>(null);

  useEffect(() => {
    // Only import on client side
    import('@/components/auth/login-page')
      .then((mod) => {
        setLoginComponent(() => mod.LoginPage);
      })
      .catch((err) => {
        console.error('Failed to load login page:', err);
      });
  }, []);

  if (!LoginComponent) {
    return <LoadingState />;
  }

  return <LoginComponent />;
}
