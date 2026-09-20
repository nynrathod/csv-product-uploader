import { useEffect, useState } from 'react';
import type { ProductFilters } from '../types';
import { useFileUpload } from '../hooks/useFileUpload';
import { useImportProgress } from '../hooks/useImportProgress';
import { useImportsData } from '../hooks/useImportsData';
import { useProductsData } from '../hooks/useProductsData';
import { ImportList } from '../components/ImportList';
import { UploadView } from '../components/upload/UploadView';
import { ProcessingView } from '../components/upload/ProcessingView';
import { ProductsView } from '../components/products/ProductsView';
import { ReportView } from '../components/ReportView';
import { PipelineMetrics } from '../components/PipelineMetrics';

type View = 'list' | 'upload' | 'processing' | 'products' | 'report';

export function Dashboard() {
  const [view, setView] = useState<View>('list');
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [filters, setFilters] = useState<ProductFilters>({
    sortBy: 'name',
    sortOrder: 'ASC',
    limit: 20,
  });

  const importsState = useImportsData();
  const upload = useFileUpload();
  const progress = useImportProgress(
    view === 'processing' ? activeJobId : null,
  );
  const productsState = useProductsData(
    view === 'products' ? activeJobId : null,
    filters,
    page,
  );

  // A started upload is followed through its lifecycle.
  useEffect(() => {
    if (upload.activeJobId) {
      setActiveJobId(upload.activeJobId);
      setView('processing');
    }
  }, [upload.activeJobId]);

  // When the import settles, show its products or return to the list.
  useEffect(() => {
    if (view !== 'processing' || !progress.isComplete || !progress.job) return;
    const job = progress.job;
    const t = setTimeout(() => {
      if (job.status === 'completed') {
        setPage(1);
        setView('products');
      } else {
        setView('list');
        importsState.refresh();
      }
    }, 900);
    return () => clearTimeout(t);
  }, [view, progress.isComplete, progress.job, importsState.refresh]); // eslint-disable-line react-hooks/exhaustive-deps

  const backToList = () => {
    setView('list');
    importsState.refresh();
  };

  switch (view) {
    case 'upload':
      return (
        <UploadView
          pendingFile={upload.pendingFile}
          isUploading={upload.isUploading}
          error={upload.error}
          onFileSelect={upload.onFileSelect}
          onClearFile={upload.onClearFile}
          onStartUpload={upload.onStartUpload}
          onCancel={backToList}
          setError={upload.setError}
        />
      );

    case 'processing':
      return (
        <ProcessingView
          job={progress.job}
          isComplete={progress.isComplete}
          onBack={backToList}
        />
      );

    case 'products':
      if (!activeJobId) return null;
      return (
        <ProductsView
          jobId={activeJobId}
          products={productsState.products}
          pagination={productsState.pagination}
          loading={productsState.loading}
          filters={filters}
          onFilterChange={(u) => {
            setPage(1);
            setFilters((f) => ({ ...f, ...u }));
          }}
          onPageChange={setPage}
          onSort={(sortBy, sortOrder) => {
            setPage(1);
            setFilters((f) => ({ ...f, sortBy, sortOrder }));
          }}
          onLimitChange={(limit) => {
            setPage(1);
            setFilters((f) => ({ ...f, limit }));
          }}
          onBack={backToList}
          onUploadNew={() => setView('upload')}
        />
      );

    case 'report':
      if (!activeJobId) return null;
      return <ReportView jobId={activeJobId} onBack={backToList} />;

    default:
      return (
        <>
          <PipelineMetrics />
          <ImportList
            imports={importsState.imports}
            loading={importsState.loading}
            onViewProducts={(id) => {
              setActiveJobId(id);
              setPage(1);
              setView('products');
            }}
            onViewProgress={(id) => {
              setActiveJobId(id);
              setView('processing');
            }}
            onViewReport={(id) => {
              setActiveJobId(id);
              setView('report');
            }}
            onUploadNew={() => setView('upload')}
          />
        </>
      );
  }
}
