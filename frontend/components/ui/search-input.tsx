'use client'

import { useState, useEffect, useRef } from 'react'
import { Search, X } from 'lucide-react'

interface SearchInputProps {
  placeholder?: string
  value?: string
  onChange: (value: string) => void
  debounceMs?: number
  className?: string
}

export function SearchInput({
  placeholder = 'Search...',
  value: externalValue,
  onChange,
  debounceMs = 150,
  className = '',
}: SearchInputProps) {
  const [localValue, setLocalValue] = useState(externalValue ?? '')
  const debounceRef = useRef<NodeJS.Timeout | null>(null)

  // Sync with external value if controlled
  useEffect(() => {
    if (externalValue !== undefined && externalValue !== localValue) {
      setLocalValue(externalValue)
    }
  }, [externalValue])

  const handleChange = (newValue: string) => {
    setLocalValue(newValue)

    // Clear existing timeout
    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }

    // Debounce the callback
    debounceRef.current = setTimeout(() => {
      onChange(newValue)
    }, debounceMs)
  }

  const handleClear = () => {
    setLocalValue('')
    onChange('')
  }

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current)
      }
    }
  }, [])

  return (
    <div className={`relative ${className}`}>
      <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
      <input
        type="text"
        value={localValue}
        onChange={(e) => handleChange(e.target.value)}
        placeholder={placeholder}
        className="w-full pl-9 pr-8 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
      />
      {localValue && (
        <button
          type="button"
          onClick={handleClear}
          className="absolute right-2 top-1/2 -translate-y-1/2 p-1 text-gray-400 hover:text-gray-600 rounded"
        >
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  )
}
