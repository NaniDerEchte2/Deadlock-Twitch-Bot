import { useEffect, useState } from 'react';
import { Search } from 'lucide-react';

interface SearchInputProps {
  placeholder?: string;
  defaultValue?: string;
  onDebouncedChange: (value: string) => void;
}

export function SearchInput({
  placeholder = 'Suchen …',
  defaultValue = '',
  onDebouncedChange,
}: SearchInputProps) {
  const [value, setValue] = useState(defaultValue);

  useEffect(() => {
    setValue(defaultValue);
  }, [defaultValue]);

  useEffect(() => {
    const timer = window.setTimeout(() => onDebouncedChange(value.trim()), 220);
    return () => window.clearTimeout(timer);
  }, [onDebouncedChange, value]);

  return (
    <label className="relative block">
      <Search className="pointer-events-none absolute left-4 top-1/2 h-4 w-4 -translate-y-1/2 text-text-secondary" />
      <input
        value={value}
        onChange={(event) => setValue(event.target.value)}
        placeholder={placeholder}
        className="admin-input pl-11"
      />
    </label>
  );
}
