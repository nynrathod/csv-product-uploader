import type { AxiosResponse, AxiosError } from 'axios';

/**
 * Response interceptor - normalizes responses and errors
 */
export const onResponseFulfilled = (response: AxiosResponse) => {
    if (import.meta.env.DEV) {
        console.log('%c Response: ', 'background: #0f0; color: #fff', response);
    }
    return response;
};

export const onResponseRejected = (error: AxiosError<{ message?: string }>) => {
    if (import.meta.env.DEV) {
        console.log('%c Error: ', 'background: #f00; color: #fff', error.response);
    }

    const message =
        error?.response?.data?.message ||
        error?.response?.statusText ||
        'Network error. Please try again.';

    return Promise.reject({ ...error.response, message });
};
