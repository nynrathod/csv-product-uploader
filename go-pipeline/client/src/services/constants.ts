// Empty base targets the same origin, which the dev server proxies to
// the importer; set VITE_API_URL to target a deployed importer directly.
export const API_BASE: string =
  (import.meta.env.VITE_API_URL as string | undefined) ?? '';
