import { Header } from './components/layout/Header';
import { AuthLayout } from './components/layout/AuthLayout';
import { Dashboard } from './pages/Dashboard';
import { useAuth } from './hooks/useAuth';
import { cn } from './lib/utils';

/**
 * Main App component - handles authentication and layout
 */
export default function App() {
  const {
    isAuthenticated,
    userId,
    isLoading,
    error,
    signup,
    login,
    logout,
    clearError,
  } = useAuth();

  const containerClass = 'w-full max-w-7xl mx-auto px-4 sm:px-6 lg:px-8';

  // Show auth layout when not authenticated
  if (!isAuthenticated) {
    return (
      <AuthLayout
        isLoading={isLoading}
        error={error}
        onSignup={signup}
        onLogin={login}
        onClearError={clearError}
      />
    );
  }

  // Authenticated layout
  return (
    <div className="flex h-screen flex-col overflow-hidden bg-white">
      <Header userId={userId} onLogout={logout} />

      <div className="flex-1 overflow-hidden">
        <main className={cn('h-full py-6', containerClass)}>
          <Dashboard />
        </main>
      </div>
    </div>
  );
}
