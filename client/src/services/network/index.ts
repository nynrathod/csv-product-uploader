import axios from 'axios';
import type { AxiosInstance } from 'axios';
import { onResponseFulfilled, onResponseRejected } from './response';
import { onRequestFulfilled, onRequestRejected } from './request';
import { authService } from '../auth';

interface NetworkRequest {
    url: string;
    body?: unknown;
    headers?: Record<string, string>;
    params?: Record<string, unknown>;
}

/**
 * Network service - centralized HTTP client with interceptors
 */
class Network {
    client: AxiosInstance;

    constructor() {
        this.client = axios.create({
            baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
            timeout: 300000, // 5 minutes for large file uploads
        });
        this.attachInterceptors();
        this.attachAuthToken();
    }

    get = async <T>(request: NetworkRequest) => {
        return this.client.get<T>(request.url, {
            params: request.params,
            headers: request.headers,
        });
    };

    post = async <T>(request: NetworkRequest) => {
        return this.client.post<T>(request.url, request.body, {
            params: request.params,
            headers: request.headers,
        });
    };

    // Attaches auth token from localStorage to all requests
    private attachAuthToken = () => {
        this.client.interceptors.request.use((config) => {
            const token = authService.getToken();
            if (token) {
                config.headers.Authorization = `Bearer ${token}`;
            }
            return config;
        });
    };

    private attachInterceptors = () => {
        this.client.interceptors.request.use(onRequestFulfilled, onRequestRejected);
        this.client.interceptors.response.use(
            onResponseFulfilled,
            onResponseRejected,
        );
    };
}

const network = new Network();
export default network;
