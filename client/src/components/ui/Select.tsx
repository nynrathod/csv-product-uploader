import { useState, useRef, useEffect } from 'react';
import { cn } from '../../lib/utils';
import ChevronDown from '../../assets/icons/chevron-down.svg?react';
import Check from '../../assets/icons/check.svg?react';

interface SelectOption {
  value: string | number;
  label: string;
}

interface SelectProps {
  value: string | number;
  onChange: (value: string | number) => void;
  options: SelectOption[];
  className?: string;
}

export function Select({ value, onChange, options, className }: SelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const selectedOption = options.find((opt) => opt.value === value);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div ref={containerRef} className={cn('relative', className)}>
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="flex h-8 min-w-[70px] items-center justify-between gap-2 rounded-md border border-zinc-200 bg-white px-3 text-sm font-medium hover:bg-zinc-50 focus:ring-2 focus:ring-zinc-900 focus:ring-offset-1 focus:outline-none"
      >
        <span>{selectedOption?.label}</span>
        <ChevronDown
          className={cn(
            'h-4 w-4 text-zinc-500 transition-transform',
            isOpen && 'rotate-180',
          )}
        />
      </button>

      {isOpen && (
        <div className="absolute bottom-full left-0 z-[100] mb-1 min-w-[120px] rounded-md border border-zinc-200 bg-white py-1 shadow-lg">
          {options.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => {
                onChange(option.value);
                setIsOpen(false);
              }}
              className={cn(
                'flex w-full items-center justify-between px-3 py-2 text-sm hover:bg-zinc-100',
                value === option.value && 'bg-zinc-50 font-medium',
              )}
            >
              <span>{option.label}</span>
              {value === option.value && (
                <Check className="h-4 w-4 text-zinc-900" />
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
