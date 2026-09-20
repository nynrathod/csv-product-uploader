import { useState } from 'react';
import { Button } from '../ui/Button';
import AlertTriangle from '../../assets/icons/alert-triangle.svg?react';

interface AuthLayoutProps {
    isLoading: boolean;
    error: string | null;
    onSignup: () => void;
    onLogin: (userId: string) => void;
    onClearError: () => void;
}

export function AuthLayout({
    isLoading,
    error,
    onSignup,
    onLogin,
    onClearError,
}: AuthLayoutProps) {
    const [loginId, setLoginId] = useState('');
    const [isLoginView, setIsLoginView] = useState(false);

    const handleSubmit = () => {
        if (isLoginView && loginId.trim()) {
            onLogin(loginId.trim());
            setLoginId('');
        } else if (!isLoginView) {
            onSignup();
        }
    };

    const toggleView = () => {
        setIsLoginView(!isLoginView);
        onClearError();
        setLoginId('');
    };

    // Error alert component
    const errorAlert = error ? (
        <div className="flex items-center gap-2 rounded-md border border-red-100 bg-red-50 p-3 text-sm text-red-600">
            <AlertTriangle className="h-4 w-4 text-red-600" />
            {error}
            <button className="ml-auto" onClick={onClearError}>×</button>
        </div>
    ) : null;

    // Signup button
    const signupButton = (
        <Button className="w-full" size="lg" onClick={onSignup} disabled={isLoading}>
            {isLoading ? 'Creating Account...' : 'Create New Account'}
        </Button>
    );

    // Login form
    const loginForm = (
        <div className="space-y-2">
            <input
                autoFocus
                className="flex h-10 w-full rounded-md border border-zinc-200 bg-white px-3 py-2 text-sm ring-offset-white placeholder:text-zinc-500 focus-visible:ring-2 focus-visible:ring-zinc-950 focus-visible:ring-offset-2 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50"
                placeholder="Enter your User ID"
                value={loginId}
                onChange={(e) => setLoginId(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSubmit()}
            />
            <Button
                className="w-full"
                onClick={handleSubmit}
                disabled={isLoading || !loginId.trim()}
            >
                {isLoading ? 'Logging in...' : 'Login'}
            </Button>
        </div>
    );

    return (
        <div className="flex min-h-screen items-center justify-center bg-zinc-50/50">
            <div className="w-full max-w-sm space-y-6 p-6">
                <div className="flex flex-col space-y-2 text-center">
                    <h1 className="text-2xl font-semibold tracking-tight">Welcome back</h1>
                    <p className="text-sm text-zinc-500">Enter your credentials to continue</p>
                </div>

                {errorAlert}

                <div className="space-y-4">
                    {isLoginView ? loginForm : signupButton}

                    <div className="text-center">
                        <button
                            className="cursor-pointer text-sm text-zinc-500 underline-offset-4 hover:text-zinc-900 hover:underline"
                            onClick={toggleView}
                        >
                            {isLoginView ? 'Need an account? Sign up' : 'Already have an ID? Login'}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
