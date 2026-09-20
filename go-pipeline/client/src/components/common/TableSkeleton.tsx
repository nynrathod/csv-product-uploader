interface TableSkeletonProps {
    rows?: number;
    columns: number[];
}

/**
 * Reusable table skeleton loader for loading states
 */
export function TableSkeleton({ rows = 3, columns }: TableSkeletonProps) {
    return (
        <>
            {Array.from({ length: rows }).map((_, rowIndex) => (
                <tr
                    key={rowIndex}
                    className="border-b border-zinc-100 transition-colors hover:bg-zinc-50/50"
                >
                    {columns.map((width, colIndex) => (
                        <td key={colIndex} className="p-4">
                            <div
                                className="h-4 animate-pulse rounded bg-zinc-100"
                                style={{ width: `${width}px` }}
                            />
                        </td>
                    ))}
                </tr>
            ))}
        </>
    );
}
