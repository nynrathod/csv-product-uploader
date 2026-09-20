import { jwtDecode } from 'jwt-decode';

// Obfuscated key for 'token' (pre-calculated base64 of 'token')
const STORAGE_KEY = 'dG9rZW4=';

export interface DecodedToken {
    sub: string;
    userId?: string;
    exp: number;
    iat: number;
}

export const authService = {
    // Store session
    setSession: (token: string) => {
        try {
            localStorage.setItem(STORAGE_KEY, token);
        } catch (e) {
            console.error('Error storing token', e);
        }
    },

    // Read token
    getToken: (): string | null => {
        return localStorage.getItem(STORAGE_KEY);
    },

    // Get User ID from token
    getUserId: (): string | null => {
        const token = authService.getToken();
        if (!token) return null;
        try {
            const decoded = jwtDecode<DecodedToken>(token);
            return decoded.userId || decoded.sub;
        } catch {
            return null;
        }
    },

    // Decode token
    decodeToken: (token: string): DecodedToken | null => {
        try {
            return jwtDecode<DecodedToken>(token);
        } catch (error) {
            console.error('Error decoding token:', error);
            return null;
        }
    },

    // Validate token
    isValid: (token: string | null): boolean => {
        if (!token) return false;
        try {
            const decoded = jwtDecode<DecodedToken>(token);
            const currentTime = Date.now() / 1000;
            return decoded.exp > currentTime;
        } catch {
            return false;
        }
    },

    // Clear session
    logout: () => {
        localStorage.removeItem(STORAGE_KEY);
        localStorage.removeItem('token');
        localStorage.removeItem('userId');
    },
};
