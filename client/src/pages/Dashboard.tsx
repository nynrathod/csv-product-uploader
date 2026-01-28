import { useState, useCallback, useEffect } from 'react';

import { BatchList } from '../components/BatchList';
import { UploadView } from '../components/upload/UploadView';
import { ProcessingView } from '../components/upload/ProcessingView';
import { ProductsView } from '../components/products/ProductsView';
import { useFileUpload } from '../hooks/useFileUpload';
import { useProgressTracking } from '../hooks/useProgressTracking';
import { useProductsData } from '../hooks/useProductsData';
import { useBatchesData } from '../hooks/useBatchesData';

type ViewMode = 'batches' | 'products' | 'upload';

export function Dashboard() {
    const [viewMode, setViewMode] = useState<ViewMode>('batches');
    const [selectedBatchId, setSelectedBatchId] = useState<string | null>(null);
    const [processingBatchId, setProcessingBatchId] = useState<string | null>(null);
    const [pendingFile, setPendingFile] = useState<File | null>(null);

    const { isUploading, error, upload, reset, setError } = useFileUpload();
    const { progress, isComplete, isFailed } = useProgressTracking(processingBatchId);
    const { batches, loading: batchesLoading, refresh: refreshBatches } = useBatchesData();
    const {
        products,
        loading: productsLoading,
        filters,
        pagination,
        updateFilters,
        changePage,
        updateSort,
    } = useProductsData(selectedBatchId || undefined);

    // Handle file selection (stores file, doesn't upload)
    const handleFileSelect = useCallback(
        (selectedFile: File) => {
            setPendingFile(selectedFile);
            setError(null);
        },
        [setError],
    );

    // Handle upload start
    const handleStartUpload = async () => {
        if (!pendingFile) return;
        try {
            const result = await upload(pendingFile);
            setProcessingBatchId(result.id);
            setPendingFile(null);
        } catch {
            // Error handled by useFileUpload
        }
    };

    // Handle completion - transition to products view
    useEffect(() => {
        if (isComplete && processingBatchId) {
            setSelectedBatchId(processingBatchId);
            setProcessingBatchId(null);
            setViewMode('products');
            refreshBatches();
            reset();
        }
    }, [isComplete, processingBatchId, refreshBatches, reset]);

    // Navigation handlers
    const handleViewProducts = (batchId: string) => {
        setSelectedBatchId(batchId);
        setViewMode('products');
    };

    const handleBackToBatches = () => {
        setSelectedBatchId(null);
        setViewMode('batches');
    };

    const handleUploadNew = () => {
        reset();
        setPendingFile(null);
        setError(null);
        setViewMode('upload');
    };

    const handleCancelUpload = () => {
        reset();
        setPendingFile(null);
        setViewMode('batches');
    };

    const handleViewProgress = (batchId: string) => {
        setProcessingBatchId(batchId);
    };

    const handleBackFromProcessing = () => {
        setProcessingBatchId(null);
        reset();
    };

    // Computed flags
    const isProcessing = !!processingBatchId && !isComplete && !isFailed;
    const currentBatch = batches.find((b) => b.id === selectedBatchId);
    const showUploadView = viewMode === 'upload' && !isProcessing;
    const showBatchList = viewMode === 'batches' && !isProcessing;
    const showProductsView = viewMode === 'products' && selectedBatchId && !isProcessing;

    // Render based on computed flags
    if (isProcessing) {
        return (
            <ProcessingView
                progress={progress}
                isFailed={isFailed}
                onBack={handleBackFromProcessing}
                onRetry={handleUploadNew}
            />
        );
    }

    if (showUploadView) {
        return (
            <UploadView
                pendingFile={pendingFile}
                isUploading={isUploading}
                error={error}
                onFileSelect={handleFileSelect}
                onClearFile={() => setPendingFile(null)}
                onStartUpload={handleStartUpload}
                onCancel={handleCancelUpload}
                setError={setError}
            />
        );
    }

    if (showProductsView) {
        return (
            <ProductsView
                products={products}
                loading={productsLoading}
                filters={filters}
                pagination={pagination}
                currentBatch={currentBatch}
                onFilterChange={updateFilters}
                onPageChange={changePage}
                onSort={updateSort}
                onLimitChange={(limit) => updateFilters({ limit })}
                onBackToBatches={handleBackToBatches}
                onUploadNew={handleUploadNew}
            />
        );
    }

    if (showBatchList) {
        return (
            <div className="animate-in fade-in slide-in-from-bottom-2 flex h-full flex-col duration-500">
                <BatchList
                    batches={batches}
                    loading={batchesLoading}
                    onViewProducts={handleViewProducts}
                    onViewProgress={handleViewProgress}
                    onUploadNew={handleUploadNew}
                />
            </div>
        );
    }

    return null;
}
