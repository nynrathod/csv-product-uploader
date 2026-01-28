import { Button } from '../ui/Button';
import { cn } from '../../lib/utils';
import LogoIcon from '../../assets/icons/logo.svg?react';

interface HeaderProps {
    userId: string | null;
    onLogout: () => void;
}

export function Header({ userId, onLogout }: HeaderProps) {
    const containerClass = 'w-full max-w-7xl mx-auto px-4 sm:px-6 lg:px-8';

    return (
        <header className="z-50 flex-none border-b border-zinc-200 bg-white/75 backdrop-blur">
            <div className={cn('flex h-16 items-center justify-between', containerClass)}>
                {/* Logo */}
                <div className="flex items-center gap-2">
                    <div className="flex h-6 w-6 items-center justify-center rounded-md bg-zinc-900">
                        <LogoIcon className="h-3.5 w-3.5 text-white" />
                    </div>
                    <span className="text-sm font-semibold tracking-tight text-zinc-900">
                        Products Import
                    </span>
                </div>

                {/* User Menu */}
                <div className="flex items-center gap-4">
                    <div className="flex items-center gap-2 text-sm text-zinc-500">
                        <div className="flex h-8 w-8 items-center justify-center rounded-full border border-zinc-200 bg-zinc-100 text-xs font-medium text-zinc-900">
                            {userId?.slice(0, 2).toUpperCase()}
                        </div>
                    </div>
                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={onLogout}
                        className="text-zinc-500 hover:text-zinc-900"
                    >
                        Logout
                    </Button>
                </div>
            </div>
        </header>
    );
}
