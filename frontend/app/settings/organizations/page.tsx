'use client';

import { useState } from 'react';
import { Building2, Plus, Users } from 'lucide-react';

export default function OrganizationsSettingsPage() {
  const [organizations] = useState<any[]>([]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold text-gray-900">Organizations</h2>
          <p className="text-gray-600 mt-1">Manage your organizations and team members</p>
        </div>
        <button className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
          <Plus className="w-4 h-4" />
          Create Organization
        </button>
      </div>

      {organizations.length === 0 ? (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-12 text-center">
          <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-gray-100 flex items-center justify-center">
            <Building2 className="w-8 h-8 text-gray-400" />
          </div>
          <h3 className="text-lg font-medium text-gray-900 mb-2">No Organizations</h3>
          <p className="text-gray-600 mb-6 max-w-md mx-auto">
            Organizations allow you to collaborate with team members and share dashboards, tables, and schemas.
          </p>
          <button className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            <Plus className="w-4 h-4" />
            Create Your First Organization
          </button>
        </div>
      ) : (
        <div className="space-y-4">
          {organizations.map((org) => (
            <div key={org.id} className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-blue-100 flex items-center justify-center">
                    <Building2 className="w-5 h-5 text-blue-600" />
                  </div>
                  <div>
                    <h3 className="font-medium text-gray-900">{org.name}</h3>
                    <p className="text-sm text-gray-500">{org.slug}</p>
                  </div>
                </div>
                <div className="flex items-center gap-2 text-sm text-gray-500">
                  <Users className="w-4 h-4" />
                  <span>{org.member_count} members</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
