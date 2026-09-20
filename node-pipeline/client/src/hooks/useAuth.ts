import { useState, useCallback, useEffect } from 'react';
import { signup, login } from '../services/api';
import { authService } from '../services/auth';

interface AuthState {
  isAuthenticated: boolean;
  userId: string | null;
  token: string | null;
  isLoading: boolean;
  error: string | null;
}

export function useAuth() {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    userId: null,
    token: null,
    isLoading: false,
    error: null,
  });

  useEffect(() => {
    const token = authService.getToken();
    const userId = authService.getUserId();

    // Check if token exists and is valid (not expired)
    if (token && userId && authService.isValid(token)) {
      setState((prev) => ({
        ...prev,
        isAuthenticated: true,
        userId,
        token,
      }));
    } else if (token) {
      // Token exists but invalid/expired - clear it
      authService.logout();
    }
  }, []);

  const handleSignup = useCallback(async () => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }));
    try {
      const result = await signup();
      // Only set token, userId is derived
      authService.setSession(result.token);
      setState({
        isAuthenticated: true,
        userId: result.userId,
        token: result.token,
        isLoading: false,
        error: null,
      });
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Signup failed';
      setState((prev) => ({ ...prev, isLoading: false, error: message }));
      throw err;
    }
  }, []);

  const handleLogin = useCallback(async (userId: string) => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }));
    try {
      const result = await login(userId);
      // Only set token, userId is derived
      authService.setSession(result.token);
      setState({
        isAuthenticated: true,
        userId: result.userId,
        token: result.token,
        isLoading: false,
        error: null,
      });
      return result;
    } catch (err: unknown) {
      let message = 'User not found';
      if (err && typeof err === 'object' && 'response' in err) {
        const axiosErr = err as { response?: { status?: number } };
        if (axiosErr.response?.status === 401) {
          message = 'User ID does not exist';
        }
      }
      setState((prev) => ({ ...prev, isLoading: false, error: message }));
      throw new Error(message);
    }
  }, []);

  const logout = useCallback(() => {
    authService.logout();
    setState({
      isAuthenticated: false,
      userId: null,
      token: null,
      isLoading: false,
      error: null,
    });
  }, []);

  const clearError = useCallback(() => {
    setState((prev) => ({ ...prev, error: null }));
  }, []);

  return {
    ...state,
    signup: handleSignup,
    login: handleLogin,
    logout,
    clearError,
  };
}
