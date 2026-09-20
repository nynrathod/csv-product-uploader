import LogoIcon from '../../assets/icons/logo.svg?react';

export function Header() {
  return (
    <header className="z-50 flex-none border-b border-zinc-200 bg-white/75 backdrop-blur">
      <div className="mx-auto flex h-16 w-full max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-2">
          <div className="flex h-6 w-6 items-center justify-center rounded-md bg-zinc-900">
            <LogoIcon className="h-3.5 w-3.5 text-white" />
          </div>
          <span className="text-sm font-semibold tracking-tight text-zinc-900">
            Product Import Pipeline
          </span>
        </div>
        <div className="hidden items-center gap-2 text-xs text-zinc-400 sm:flex">
          <span>importer</span>
          <span className="text-zinc-300">·</span>
          <span>catalog-worker</span>
          <span className="text-zinc-300">·</span>
          <span>kafka</span>
        </div>
      </div>
    </header>
  );
}
