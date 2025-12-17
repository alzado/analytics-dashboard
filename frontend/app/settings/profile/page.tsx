'use client';

import { useAuth } from '@/lib/contexts/auth-context';
import { User, Mail, Calendar } from 'lucide-react';

export default function ProfileSettingsPage() {
  const { user } = useAuth();

  if (!user) {
    return null;
  }

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold text-gray-900">Profile Settings</h2>
        <p className="text-gray-600 mt-1">Manage your account information</p>
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        {/* Profile Header */}
        <div className="p-6 border-b border-gray-200">
          <div className="flex items-center gap-4">
            {user.avatar_url ? (
              <img
                src={user.avatar_url}
                alt={user.name}
                className="w-20 h-20 rounded-full"
              />
            ) : (
              <div className="w-20 h-20 rounded-full bg-blue-600 flex items-center justify-center text-white text-2xl font-medium">
                {user.name.split(' ').map(n => n[0]).join('').toUpperCase().slice(0, 2)}
              </div>
            )}
            <div>
              <h3 className="text-xl font-semibold text-gray-900">{user.name}</h3>
              <p className="text-gray-600">{user.email}</p>
            </div>
          </div>
        </div>

        {/* Profile Details */}
        <div className="p-6 space-y-4">
          <div className="flex items-center gap-3 text-gray-700">
            <User className="w-5 h-5 text-gray-400" />
            <div>
              <p className="text-sm text-gray-500">Full Name</p>
              <p className="font-medium">{user.name}</p>
            </div>
          </div>

          <div className="flex items-center gap-3 text-gray-700">
            <Mail className="w-5 h-5 text-gray-400" />
            <div>
              <p className="text-sm text-gray-500">Email Address</p>
              <p className="font-medium">{user.email}</p>
            </div>
          </div>

          <div className="flex items-center gap-3 text-gray-700">
            <Calendar className="w-5 h-5 text-gray-400" />
            <div>
              <p className="text-sm text-gray-500">Account Created</p>
              <p className="font-medium">{formatDate(user.created_at)}</p>
            </div>
          </div>

          <div className="flex items-center gap-3 text-gray-700">
            <Calendar className="w-5 h-5 text-gray-400" />
            <div>
              <p className="text-sm text-gray-500">Last Login</p>
              <p className="font-medium">{formatDate(user.last_login_at)}</p>
            </div>
          </div>
        </div>

        {/* Note */}
        <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 rounded-b-lg">
          <p className="text-sm text-gray-600">
            Your profile information is managed through your Google account.
          </p>
        </div>
      </div>
    </div>
  );
}
