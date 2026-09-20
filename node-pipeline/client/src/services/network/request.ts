import type { InternalAxiosRequestConfig } from 'axios';

/**
 * Request interceptor - logs requests in development
 */
export const onRequestFulfilled = (config: InternalAxiosRequestConfig) => {
    if (import.meta.env.DEV) {
        console.log('%c Request: ', 'background: #00f; color: #fff', config);
    }
    return config;
};

export const onRequestRejected = (error: Error) => {
    return Promise.reject(error);
};
