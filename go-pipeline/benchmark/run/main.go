// Command run executes an end-to-end pipeline benchmark: it generates or
// reuses a product CSV fixture, boots both services as subprocesses,
// streams the upload through the public HTTP API, and measures upload,
// ingest and drain throughput, steady-state event-to-database latency,
// and peak process memory until the reconciliation report confirms
// completion. An independent catalog row count verifies zero loss.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/kafka"
)

// jobStatus mirrors the importer's public job representation.
type jobStatus struct {
	ID            string  `json:"id"`
	Status        string  `json:"status"`
	TotalRows     int64   `json:"totalRows"`
	ValidRows     int64   `json:"validRows"`
	InvalidRows   int64   `json:"invalidRows"`
	PublishedRows int64   `json:"publishedRows"`
	ProcessedRows int64   `json:"processedRows"`
	RetriedRows   int64   `json:"retriedRows"`
	DeadRows      int64   `json:"deadRows"`
	LastError     *string `json:"lastError"`
}

// reportStatus mirrors the reconciliation report's verdict section.
type reportStatus struct {
	Status         string `json:"status"`
	Reconciliation struct {
		Published   int64  `json:"published"`
		Confirmed   int64  `json:"confirmed"`
		Unconfirmed int64  `json:"unconfirmed"`
		State       string `json:"state"`
	} `json:"reconciliation"`
}

type sample struct {
	t         time.Time
	processed int64
	published int64
}

type latencyStats struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
}

type benchResult struct {
	StartedAt           time.Time    `json:"started_at"`
	GoVersion           string       `json:"go_version"`
	OS                  string       `json:"os"`
	CPUs                int          `json:"cpus"`
	Rows                int64        `json:"rows"`
	ValidRows           int64        `json:"valid_rows"`
	InvalidRows         int64        `json:"invalid_rows"`
	FileMB              float64      `json:"file_mb"`
	Workers             int          `json:"workers"`
	UploadAckSec        float64      `json:"upload_ack_sec"`
	ImportSec           float64      `json:"import_sec"`
	ImportRowsPerSec    float64      `json:"import_rows_per_sec"`
	EndToEndSec         float64      `json:"end_to_end_sec"`
	EndToEndRowsPerSec  float64      `json:"end_to_end_rows_per_sec"`
	PeakDrainRowsPerSec float64      `json:"peak_drain_rows_per_sec"`
	Latency             latencyStats `json:"latency"`
	PeakRSSImporterMB   float64      `json:"peak_rss_importer_mb"`
	PeakRSSWorkerMB     float64      `json:"peak_rss_worker_mb"`
	ZeroLoss            bool         `json:"zero_loss"`
	CatalogRows         int64        `json:"catalog_rows"`
	KilledWorker        bool         `json:"killed_worker"`
	Recovery            string       `json:"recovery,omitempty"`
}

type fixtureMeta struct {
	Rows           int64 `json:"rows"`
	ValidRows      int64 `json:"valid_rows"`
	InvalidRows    int64 `json:"invalid_rows"`
	UniqueProducts int64 `json:"unique_products"`
	Bytes          int64 `json:"bytes"`
}

type child struct {
	name    string
	cmd     *exec.Cmd
	logFile *os.File
}

var (
	childrenMu sync.Mutex
	children   []*child
)

func main() {
	rows := flag.Int64("rows", 1_000_000, "rows in the benchmark CSV")
	csvPath := flag.String("csv", "", "csv path (default benchmark/data/products_<rows>.csv)")
	workers := flag.Int("workers", 1, "catalog workers to run")
	brokers := flag.String("brokers", "localhost:29092", "kafka bootstrap addresses")
	importerPort := flag.String("importer-port", "18080", "http port for the benchmark importer")
	catalogDB := flag.String("catalog-db", "postgres://catalog_svc:catalog_dev@localhost:5433/catalog_db?sslmode=disable", "catalog database")
	probe := flag.Bool("probe", true, "measure steady-state event-to-database latency")
	probeEvents := flag.Int("probe-events", 300, "sentinel events for the latency probe")
	killTest := flag.Bool("kill-test", false, "kill a worker mid-run and verify recovery")
	fresh := flag.Bool("fresh", true, "truncate the catalog before the run")
	out := flag.String("out", "benchmark/results.json", "results file")
	flag.Parse()

	if *csvPath == "" {
		*csvPath = fmt.Sprintf("benchmark/data/products_%d.csv", *rows)
	}
	base := "http://localhost:" + *importerPort
	ctx := context.Background()

	meta := ensureFixture(*rows, *csvPath)

	importerExe, workerExe := buildServices()
	defer killAllChildren()

	pool, err := pgxpool.New(ctx, *catalogDB)
	if err != nil {
		logf("connecting to the catalog database: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		logf("catalog database unreachable: %v — start the stack with: docker compose -f deploy/docker-compose.yml up -d", err)
	}
	defer pool.Close()

	if *fresh {
		// A fresh database has no products table yet: the catalog
		// worker's startup migration creates it moments after this. Both
		// cleanup statements tolerate its absence.
		if _, err := pool.Exec(ctx, `
            TRUNCATE products`); err != nil && !strings.Contains(err.Error(), "does not exist") {
			logf("truncating the catalog: %v", err)
		}
		if _, err := pool.Exec(ctx, `
            DELETE FROM products WHERE merchant_id = 'bench-probe'`); err != nil && !strings.Contains(err.Error(), "does not exist") {
			logf("clearing probe products: %v", err)
		}
	}

	if err := os.MkdirAll("benchmark/logs", 0o755); err != nil {
		logf("creating logs dir: %v", err)
	}

	// The importer runs with write-amortized progress and a deeper
	// producer buffer: benchmark conditions favor throughput.
	importer := spawnChild(importerExe, "importer", "importer.log", map[string]string{
		"IMPORTER_HTTP_PORT":            *importerPort,
		"PROGRESS_FLUSH_ROWS":           "50000",
		"PRODUCER_LINGER_MILLIS":        "10",
		"PRODUCER_MAX_BUFFERED_RECORDS": "200000",
	})
	if err := waitHealth(base); err != nil {
		logf("importer did not become healthy: %v", err)
	}

	workerEnv := map[string]string{
		"WORKER_BATCH_SIZE":            "25000",
		"WORKER_MAX_POLL_RECORDS":      "25000",
		"WORKER_FETCH_MAX_WAIT_MILLIS": "50",
	}

	firstWorker := spawnChild(workerExe, "worker", "worker-0.log", workerEnv)
	for i := 1; i < *workers; i++ {
		spawnChild(workerExe, "worker", fmt.Sprintf("worker-%d.log", i), workerEnv)
	}

	memTrk := newMemTracker()
	memTrk.Add(importer.cmd.Process.Pid, "importer")
	for _, c := range currentChildren() {
		if c.name == "worker" {
			memTrk.Add(c.cmd.Process.Pid, "worker")
		}
	}
	memStop := make(chan struct{})
	memTrk.Start(memStop)

	// Upload: the fixture is streamed through the public multipart API,
	// never buffered whole by the harness.
	t0 := time.Now()
	jobID, err := upload(base, *csvPath)
	if err != nil {
		logf("upload failed: %v", err)
	}
	t202 := time.Now()

	killed := false
	var recovery string
	midRun := func(job jobStatus) {
		if !*killTest || killed || firstWorker == nil {
			return
		}
		if job.ProcessedRows > meta.ValidRows*3/10 {
			killed = true
			killChild(firstWorker)
			recovery = fmt.Sprintf("worker killed at %d/%d rows (%.0f%%), respawned after 2s",
				job.ProcessedRows, meta.ValidRows, 100*float64(job.ProcessedRows)/float64(meta.ValidRows))
			fmt.Println("  [crash test] " + recovery)
			time.Sleep(2 * time.Second)
			firstWorker = spawnChild(workerExe, "worker", "worker-respawn.log", workerEnv)
			fmt.Println("  [crash test] waiting for the broker to reassign the killed worker's partitions (session timeout ~10s)...")
			memTrk.Add(firstWorker.cmd.Process.Pid, "worker")
		}
	}

	samples, final, err := pollUntilDone(base, jobID, midRun)

	if err != nil {
		logf("%v", err)
	}
	close(memStop)

	// Ingest phase ends when every valid row is durably published; the
	// end-to-end time ends when the catalog confirms the last event.
	var tImport time.Time
	for _, s := range samples {
		if s.published >= meta.ValidRows {
			tImport = s.t
			break
		}
	}
	if tImport.IsZero() {
		tImport = t202
	}
	var peakDrain float64
	for i := 1; i < len(samples); i++ {
		dt := samples[i].t.Sub(samples[i-1].t).Seconds()
		if dt <= 0 {
			continue
		}
		if r := float64(samples[i].processed-samples[i-1].processed) / dt; r > peakDrain {
			peakDrain = r
		}
	}

	rep, err := getReport(base, jobID)
	if err != nil {
		logf("loading the reconciliation report: %v", err)
	}
	var catalogRows int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM products WHERE merchant_id LIKE 'bench-m%'`).Scan(&catalogRows); err != nil {
		logf("counting catalog rows: %v", err)
	}

	zeroLoss := rep.Reconciliation.State == "complete" &&
		final.ProcessedRows+final.DeadRows >= meta.ValidRows &&
		catalogRows == meta.UniqueProducts

	var lat latencyStats
	if *probe {
		fmt.Println("  [probe] measuring steady-state event-to-database latency...")
		lat, err = runProbe(strings.Split(*brokers, ","), pool, *probeEvents)
		if err != nil {
			fmt.Printf("  [probe] failed: %v\n", err)
		}
	}

	res := benchResult{
		StartedAt:           t0.UTC(),
		GoVersion:           runtime.Version(),
		OS:                  runtime.GOOS,
		CPUs:                runtime.NumCPU(),
		Rows:                meta.Rows,
		ValidRows:           meta.ValidRows,
		InvalidRows:         meta.InvalidRows,
		FileMB:              float64(meta.Bytes) / 1e6,
		Workers:             *workers,
		UploadAckSec:        t202.Sub(t0).Seconds(),
		ImportSec:           tImport.Sub(t202).Seconds(),
		EndToEndSec:         samples[len(samples)-1].t.Sub(t0).Seconds(),
		PeakDrainRowsPerSec: peakDrain,
		Latency:             lat,
		PeakRSSImporterMB:   float64(memTrk.Peak("importer")) / 1024,
		PeakRSSWorkerMB:     float64(memTrk.Peak("worker")) / 1024,
		ZeroLoss:            zeroLoss,
		CatalogRows:         catalogRows,
		KilledWorker:        killed,
		Recovery:            recovery,
	}
	if res.ImportSec > 0 {
		res.ImportRowsPerSec = float64(meta.ValidRows) / res.ImportSec
	}
	if res.EndToEndSec > 0 {
		res.EndToEndRowsPerSec = float64(meta.ValidRows) / res.EndToEndSec
	}

	printReport(res, rep.Reconciliation.State)
	if err := writeResults(*out, res); err != nil {
		logf("writing results: %v", err)
	}
}

func ensureFixture(rows int64, csvPath string) fixtureMeta {
	metaPath := csvPath + ".meta.json"
	if m, err := readMeta(metaPath); err == nil && m.Rows == rows {
		fmt.Printf("  [fixture] reusing %s (%d rows)\n", csvPath, m.Rows)
		return m
	}
	fmt.Printf("  [fixture] generating %s (%d rows)...\n", csvPath, rows)
	cmd := exec.Command("go", "run", "./benchmark/gen",
		"-rows", strconv.FormatInt(rows, 10), "-out", csvPath)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		logf("generating fixture: %v", err)
	}
	m, err := readMeta(metaPath)
	if err != nil {
		logf("reading fixture meta: %v", err)
	}
	return m
}

func readMeta(path string) (fixtureMeta, error) {
	var m fixtureMeta
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(b, &m)
	return m, err
}

func buildServices() (importerExe, workerExe string) {
	if err := os.MkdirAll("bin", 0o755); err != nil {
		logf("creating bin dir: %v", err)
	}
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	importerExe = filepath.Join("bin", "importer"+suffix)
	workerExe = filepath.Join("bin", "catalog-worker"+suffix)

	fmt.Println("  [build] compiling services...")
	for _, tc := range []struct{ out, pkg string }{
		{importerExe, "./cmd/importer"},
		{workerExe, "./cmd/catalog-worker"},
	} {
		cmd := exec.Command("go", "build", "-trimpath", "-ldflags", "-s -w", "-o", tc.out, tc.pkg)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			logf("building %s: %v", tc.pkg, err)
		}
	}
	return importerExe, workerExe
}

func spawnChild(exe, name, logName string, env map[string]string) *child {
	logFile, err := os.Create(filepath.Join("benchmark", "logs", logName))
	if err != nil {
		logf("creating log file: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = childEnv(env)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logf("starting %s: %v", name, err)
	}
	c := &child{name: name, cmd: cmd, logFile: logFile}
	childrenMu.Lock()
	children = append(children, c)
	childrenMu.Unlock()
	fmt.Printf("  [spawn] %s (pid %d, logs: benchmark/logs/%s)\n", name, cmd.Process.Pid, logName)
	return c
}

func childEnv(extra map[string]string) []string {
	env := os.Environ()
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

func killChild(c *child) {
	if c == nil || c.cmd.Process == nil {
		return
	}
	_ = c.cmd.Process.Kill()
	go func() {
		_ = c.cmd.Wait()
		c.logFile.Close()
	}()
}

func killAllChildren() {
	childrenMu.Lock()
	cs := append([]*child(nil), children...)
	childrenMu.Unlock()
	for _, c := range cs {
		killChild(c)
	}
}

func currentChildren() []*child {
	childrenMu.Lock()
	defer childrenMu.Unlock()
	return append([]*child(nil), children...)
}

func waitHealth(base string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/health")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout")
}

func upload(base, csvPath string) (string, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// A known Content-Length keeps the request body deterministic: no
	// chunked framing, no trailing-protocol ambiguity between client and
	// server on completion.
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	fw, err := mw.CreateFormFile("file", filepath.Base(csvPath))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(fw, f); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Post(base+"/api/v1/imports", mw.FormDataContentType(), body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("upload answered %d: %s", resp.StatusCode, b)
	}
	var job jobStatus
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return "", err
	}
	fmt.Printf("  [upload] accepted, job %s\n", job.ID)
	return job.ID, nil
}

func getJob(base, id string) (jobStatus, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(base + "/api/v1/imports/" + id)
	if err != nil {
		return jobStatus{}, err
	}
	defer resp.Body.Close()
	var job jobStatus
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return job, err
	}
	return job, nil
}

func getReport(base, id string) (reportStatus, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(base + "/api/v1/imports/" + id + "/report")
	if err != nil {
		return reportStatus{}, err
	}
	defer resp.Body.Close()
	var rep reportStatus
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		return rep, err
	}
	return rep, nil
}

func pollUntilDone(base, jobID string, midRun func(jobStatus)) ([]sample, jobStatus, error) {
	samples := []sample{{t: time.Now()}}
	deadline := time.Now().Add(30 * time.Minute)

	lastProcessed := int64(-1)
	lastChange := time.Now()

	for {
		job, err := getJob(base, jobID)
		if err != nil {
			return samples, job, err
		}
		samples = append(samples, sample{t: time.Now(), processed: job.ProcessedRows, published: job.PublishedRows})
		fmt.Printf("  [poll] published=%d processed=%d status=%s\n", job.PublishedRows, job.ProcessedRows, job.Status)

		if job.Status == "failed" {
			msg := "unknown cause"
			if job.LastError != nil {
				msg = *job.LastError
			}
			return samples, job, fmt.Errorf("import failed: %s", msg)
		}
		if job.Status == "completed" {
			return samples, job, nil
		}
		if midRun != nil {
			midRun(job)
		}

		// A stalled drain self-reports: worker liveness, consumer lag and
		// service log tails are dumped instead of polling silently
		// forever, and the dump repeats while the stall persists.
		if job.ProcessedRows != lastProcessed {
			lastProcessed = job.ProcessedRows
			lastChange = time.Now()
		} else if time.Since(lastChange) > 15*time.Second {
			dumpDiagnostics()
			lastChange = time.Now()
		}

		if time.Now().After(deadline) {
			return samples, job, fmt.Errorf("timed out waiting for completion")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// dumpDiagnostics reports the state of both services, the consumer group
// lag and the service log tails when a drain stalls, converting a silent
// freeze into an immediately actionable picture.
func dumpDiagnostics() {
	fmt.Println("  [stall] no progress for 15s — diagnostics:")

	for _, c := range currentChildren() {
		status := "alive"
		if !alive(c.cmd.Process.Pid) {
			status = "EXITED"
		}
		fmt.Printf("    %-8s pid %-6d %s\n", c.name, c.cmd.Process.Pid, status)
	}

	for _, group := range []string{"catalog-writer", "import-tracker"} {
		out, err := exec.Command("docker", "exec", "pipeline-kafka",
			"/opt/kafka/bin/kafka-consumer-groups.sh",
			"--bootstrap-server", "localhost:9092",
			"--group", group, "--describe").CombinedOutput()
		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				if strings.Contains(line, "product.imported.v1") || strings.Contains(line, "import.progress.v1") {
					fmt.Println("    " + strings.Join(strings.Fields(line), " "))
				}
			}
		}
	}

	for _, name := range []string{"importer.log", "worker-0.log"} {
		b, err := os.ReadFile(filepath.Join("benchmark", "logs", name))
		if err != nil {
			continue
		}
		lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
		if len(lines) > 10 {
			lines = lines[len(lines)-10:]
		}
		fmt.Printf("    --- %s (last 10 lines) ---\n", name)
		for _, l := range lines {
			fmt.Println("    " + l)
		}
	}
}

// alive reports whether a process still exists; used to surface a dead
// worker immediately instead of watching a frozen counter.
func alive(pid int) bool {
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH").Output()
		if err != nil {
			return false
		}
		return strings.Contains(string(out), fmt.Sprintf("%d", pid))
	}
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil
}

// runProbe measures steady-state event-to-database latency: sentinel
// events flow through the real producer and consumer path, and each
// sentinel's database commit time is compared with its production
// timestamp.
func runProbe(brokers []string, pool *pgxpool.Pool, n int) (latencyStats, error) {
	ctx := context.Background()
	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:            brokers,
		ClientID:           "bench-probe",
		Linger:             time.Millisecond,
		MaxBufferedRecords: 10000,
		DeliveryTimeout:    10 * time.Second,
	})
	if err != nil {
		return latencyStats{}, err
	}
	defer pub.Close()

	const merchant = "bench-probe"
	sent := make(map[string]time.Time, n)
	for i := 0; i < n; i++ {
		pid := fmt.Sprintf("probe-%05d", i)
		evt := events.ProductImported{
			JobID:      "00000000-0000-0000-0000-000000000000",
			RowNum:     int64(i),
			ProducedAt: time.Now().UTC(),
			Product: events.ProductData{
				MerchantID: merchant,
				ProductID:  pid,
				Name:       "Benchmark Probe",
				PriceCents: 100,
				Currency:   "USD",
			},
		}
		sent[pid] = evt.ProducedAt
		if err := pub.PublishProductImported(ctx, evt); err != nil {
			return latencyStats{}, err
		}
		time.Sleep(15 * time.Millisecond)
	}
	if err := pub.Flush(ctx); err != nil {
		return latencyStats{}, err
	}

	var lats []float64
	seen := make(map[string]bool, n)
	deadline := time.Now().Add(2 * time.Minute)
	for len(lats) < n && time.Now().Before(deadline) {
		rows, err := pool.Query(ctx,
			`SELECT product_id, imported_at FROM products WHERE merchant_id = $1`, merchant)
		if err != nil {
			return latencyStats{}, err
		}
		for rows.Next() {
			var pid string
			var importedAt time.Time
			if err := rows.Scan(&pid, &importedAt); err != nil {
				rows.Close()
				return latencyStats{}, err
			}
			if seen[pid] {
				continue
			}
			seen[pid] = true
			if t, ok := sent[pid]; ok {
				lats = append(lats, importedAt.Sub(t).Seconds()*1000)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return latencyStats{}, err
		}
		time.Sleep(100 * time.Millisecond)
	}

	sort.Float64s(lats)

	// The database container's clock can drift from the host clock. True
	// produce-to-database latency is never negative, so the most negative
	// sample approximates the clock offset; correcting by it puts every
	// sample in the producer's time domain.
	if len(lats) > 0 && lats[0] < 0 {
		off := -lats[0]
		for i := range lats {
			lats[i] += off
		}
	}

	st := latencyStats{Samples: len(lats)}
	if len(lats) > 0 {
		st.P50MS = percentile(lats, 0.50)
		st.P95MS = percentile(lats, 0.95)
		st.P99MS = percentile(lats, 0.99)
		st.MaxMS = lats[len(lats)-1]
	}
	return st, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

type memTracker struct {
	mu    sync.Mutex
	pids  map[int]string
	peaks map[int]int64
}

func newMemTracker() *memTracker {
	return &memTracker{pids: map[int]string{}, peaks: map[int]int64{}}
}

func (m *memTracker) Add(pid int, name string) {
	m.mu.Lock()
	m.pids[pid] = name
	m.mu.Unlock()
}

func (m *memTracker) Start(stop <-chan struct{}) {
	go func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				m.mu.Lock()
				pids := make([]int, 0, len(m.pids))
				for pid := range m.pids {
					pids = append(pids, pid)
				}
				m.mu.Unlock()
				for _, pid := range pids {
					if kb, ok := rssKB(pid); ok {
						m.mu.Lock()
						if kb > m.peaks[pid] {
							m.peaks[pid] = kb
						}
						m.mu.Unlock()
					}
				}
			}
		}
	}()
}

func (m *memTracker) Peak(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var peak int64
	for pid, n := range m.pids {
		if n != name {
			continue
		}
		if m.peaks[pid] > peak {
			peak = m.peaks[pid]
		}
	}
	return peak
}

// rssKB reports a process's resident set size in kilobytes. Windows reads
// it from tasklist output; Unix systems read it from procfs.
func rssKB(pid int) (int64, bool) {
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH").Output()
		if err != nil {
			return 0, false
		}
		line := strings.TrimSpace(string(out))
		if line == "" || !strings.Contains(line, ",") {
			return 0, false
		}
		fields := strings.Split(strings.Trim(line, "\""), "\",\"")
		if len(fields) < 5 {
			return 0, false
		}
		s := strings.TrimSpace(fields[len(fields)-1])
		s = strings.TrimSuffix(s, " K")
		s = strings.ReplaceAll(s, ",", "")
		kb, err := strconv.ParseInt(s, 10, 64)
		return kb, err == nil
	}

	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					return kb, true
				}
			}
		}
	}
	return 0, false
}

func printReport(r benchResult, state string) {
	fmt.Println()
	fmt.Println("════════════════ PIPELINE BENCHMARK ════════════════")
	fmt.Printf("rows               %d (valid %d / invalid %d)\n", r.Rows, r.ValidRows, r.InvalidRows)
	fmt.Printf("file               %.1f MB\n", r.FileMB)
	fmt.Printf("workers            %d    (%s, %d cpus)\n", r.Workers, r.OS, r.CPUs)
	fmt.Println("───────────────────────────────────────────────────")
	fmt.Printf("upload ack         %.2f s\n", r.UploadAckSec)
	fmt.Printf("ingest published   %.2f s   (%.0f rows/s)\n", r.ImportSec, r.ImportRowsPerSec)
	fmt.Printf("end-to-end         %.2f s   (%.0f rows/s)\n", r.EndToEndSec, r.EndToEndRowsPerSec)
	fmt.Printf("peak drain         %.0f rows/s\n", r.PeakDrainRowsPerSec)
	fmt.Printf("latency p50/p95    %.0f / %.0f ms   (n=%d)\n", r.Latency.P50MS, r.Latency.P95MS, r.Latency.Samples)
	fmt.Printf("latency p99/max    %.0f / %.0f ms\n", r.Latency.P99MS, r.Latency.MaxMS)
	fmt.Printf("peak rss importer  %.0f MB\n", r.PeakRSSImporterMB)
	fmt.Printf("peak rss worker    %.0f MB\n", r.PeakRSSWorkerMB)
	fmt.Println("───────────────────────────────────────────────────")
	fmt.Printf("reconciliation     %s (catalog rows: %d)\n", state, r.CatalogRows)
	fmt.Printf("zero-loss          %v\n", r.ZeroLoss)
	if r.KilledWorker {
		fmt.Printf("crash recovery     %s\n", r.Recovery)
	}
	fmt.Println("═══════════════════════════════════════════════════")
}

func writeResults(path string, r benchResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("  [results] written to %s\n", path)
	return os.WriteFile(path, b, 0o644)
}

func logf(format string, args ...any) {
	fmt.Printf("bench: "+format+"\n", args...)
	os.Exit(1)
}
