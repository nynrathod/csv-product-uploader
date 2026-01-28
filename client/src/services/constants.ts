/**
 * API endpoint constants
 */
export const API_ENDPOINTS = {
    // Auth
    AUTH_SIGNUP: '/auth/signup',
    AUTH_LOGIN: '/auth/login',

    // Upload
    UPLOAD: '/upload',
    UPLOAD_BATCHES: '/upload/batches',
    UPLOAD_PROGRESS: (batchId: string) => `/upload/progress/${batchId}`,

    // Products
    PRODUCTS: '/products',
} as const;
