import { Header } from './components/layout/Header';
import { Dashboard } from './pages/Dashboard';

export default function App() {
  return (
    <div className="flex min-h-screen flex-col bg-zinc-50">
      <Header />
      <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-8 sm:px-6 lg:px-8">
        <Dashboard />
      </main>
    </div>
  );
}
