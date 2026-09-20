// Command gen produces deterministic product CSV fixtures for pipeline
// benchmarking, with a sidecar metadata file describing the expected
// outcome so the benchmark harness can verify end-to-end correctness.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var adjectives = []string{
	"Wireless", "Ergonomic", "Compact", "Industrial", "Portable",
	"Precision", "Deluxe", "Classic", "Modern", "Rugged",
	"Sleek", "Heavy", "Duty", "Ultra", "Pro",
	"Standard", "Premium", "Lightweight", "Robust", "Versatile",
}

var nouns = []string{
	"Gadget", "Widget", "Analyzer", "Console", "Module",
	"Router", "Sensor", "Terminal", "Amplifier", "Controller",
	"Monitor", "Adapter", "Converter", "Receiver", "Transmitter",
	"Calibrator", "Projector", "Scanner", "Encoder", "Regulator",
}

var currencies = []string{"USD", "EUR", "GBP", "JPY", "INR", "AUD", "CAD", "CHF"}

// meta describes the fixture so benchmarks can assert the exact expected
// outcome instead of hardcoding expectations.
type meta struct {
	Rows           int64     `json:"rows"`
	ValidRows      int64     `json:"valid_rows"`
	InvalidRows    int64     `json:"invalid_rows"`
	UniqueProducts int64     `json:"unique_products"`
	Merchants      int       `json:"merchants"`
	Bytes          int64     `json:"bytes"`
	Seed           int64     `json:"seed"`
	GeneratedAt    time.Time `json:"generated_at"`
}

func main() {
	rows := flag.Int64("rows", 1_000_000, "total rows to generate")
	out := flag.String("out", "benchmark/data/products.csv", "output csv path")
	merchants := flag.Int("merchants", 50, "distinct merchant ids")
	invalidPermille := flag.Int("invalid-permille", 15, "invalid rows per thousand")
	seed := flag.Int64("seed", 42, "deterministic seed")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		log.Fatalf("creating output dir: %v", err)
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatalf("creating csv: %v", err)
	}

	// Every valid row carries a globally unique product id, so the
	// catalog receives the maximum insert load rather than upsert
	// deduplication.
	rng := rand.New(rand.NewSource(*seed))
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, "merchant_id,product_id,name,price,currency,expiration_date")

	var invalid int64
	for i := int64(0); i < *rows; i++ {
		merchant := fmt.Sprintf("bench-m%02d", rng.Intn(*merchants))
		productID := fmt.Sprintf("SKU-%09d", i)
		name := fmt.Sprintf("%s %s #%d",
			adjectives[rng.Intn(len(adjectives))], nouns[rng.Intn(len(nouns))], rng.Intn(999999))
		price := fmt.Sprintf("%d.%02d", rng.Intn(2000)+1, rng.Intn(100))
		currency := currencies[rng.Intn(len(currencies))]
		expiration := ""
		if rng.Intn(10) < 7 {
			expiration = fmt.Sprintf("%d-%02d-%02d", 2025+rng.Intn(5), rng.Intn(12)+1, rng.Intn(28)+1)
		}

		line := fmt.Sprintf("%s,%s,%s,%s,%s,%s", merchant, productID, name, price, currency, expiration)

		// A controlled share of rows violates field rules; row-level
		// rejection is part of the benchmarked path.
		if p := rng.Intn(1000); p < *invalidPermille {
			invalid++
			switch p % 3 {
			case 0:
				line = fmt.Sprintf("%s,%s,,%s,%s,%s", merchant, productID, price, currency, expiration)
			case 1:
				line = fmt.Sprintf("%s,%s,%s,N/A,%s,%s", merchant, productID, name, currency, expiration)
			default:
				line = fmt.Sprintf("%s,%s,%s,%s,%s,31-31-2023", merchant, productID, name, price, currency)
			}
		}

		fmt.Fprintln(w, line)
		if (i+1)%100_000 == 0 {
			log.Printf("generated %d rows", i+1)
		}
	}

	if err := w.Flush(); err != nil {
		log.Fatalf("flushing csv: %v", err)
	}
	if err := f.Close(); err != nil {
		log.Fatalf("closing csv: %v", err)
	}

	st, err := os.Stat(*out)
	if err != nil {
		log.Fatalf("stat csv: %v", err)
	}

	m := meta{
		Rows:           *rows,
		ValidRows:      *rows - invalid,
		InvalidRows:    invalid,
		UniqueProducts: *rows - invalid,
		Merchants:      *merchants,
		Bytes:          st.Size(),
		Seed:           *seed,
		GeneratedAt:    time.Now().UTC(),
	}
	metaPath := *out + ".meta.json"
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		log.Fatalf("encoding meta: %v", err)
	}
	if err := os.WriteFile(metaPath, mb, 0o644); err != nil {
		log.Fatalf("writing meta: %v", err)
	}

	log.Printf("fixture ready: %s (%.1f MB, %d rows, %d invalid)", *out, float64(m.Bytes)/1e6, m.Rows, m.InvalidRows)
	log.Printf("meta: %s", metaPath)
}
