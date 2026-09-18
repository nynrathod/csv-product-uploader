package importjob

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// ErrHeader reports a CSV file whose header row cannot be mapped onto the
// product schema: missing or duplicate columns, or an empty file.
var ErrHeader = errors.New("invalid csv header")

// dateLayout is the only accepted expiration_date format.
const dateLayout = "2006-01-02"

// requiredColumns lists every column the importer depends on. Columns may be
// supplied in any order and casing; additional columns are ignored.
var requiredColumns = []string{
	"merchant_id",
	"product_id",
	"name",
	"price",
	"currency",
	"expiration_date",
}

const (
	maxMerchantIDLen = 64
	maxProductIDLen  = 64
	maxNameLen       = 256
)

// maxDollars is the largest whole-dollar amount whose cent representation
// still fits in an int64.
const maxDollars = (1<<63 - 1 - 99) / 100

// validCurrencies restricts prices to the currencies the platform settles in.
var validCurrencies = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "JPY": {},
	"INR": {}, "AUD": {}, "CAD": {}, "CHF": {}, "CNY": {},
}

// utf8BOM is the byte-order mark prefix produced by Windows spreadsheet
// exports.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// Row is the outcome of validating and normalizing one CSV record. Reason is
// empty when the row is valid and carries the rejection cause otherwise.
type Row struct {
	Num     int64
	Product events.ProductData
	Reason  string
}

// Valid reports whether the row passed validation.
func (r Row) Valid() bool { return r.Reason == "" }

// Stats aggregates the outcome of streaming one CSV file.
type Stats struct {
	TotalRows   int64
	ValidRows   int64
	InvalidRows int64
}

// Streamer parses product CSV files with bounded memory: each record is
// validated, handed to the caller and released before the next record is
// read, so heap usage stays flat regardless of file size.
type Streamer struct{}

// NewStreamer returns a Streamer ready for use.
func NewStreamer() *Streamer { return &Streamer{} }

// Stream consumes src completely, invoking emit exactly once per data
// record. It returns the aggregate stats and the first error encountered:
// a context cancellation, an error returned by emit, or a header problem.
// Malformed or invalid records are reported to emit as invalid rows and do
// not abort the stream, so a single bad line cannot fail a million-row
// import.
func (s *Streamer) Stream(ctx context.Context, src io.Reader, emit func(Row) error) (Stats, error) {
	var stats Stats

	reader := csv.NewReader(stripBOM(src))
	// ReuseRecord recycles the record buffer between reads, which keeps the
	// steady-state allocation flat while streaming large files.
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return stats, fmt.Errorf("%w: file has no header", ErrHeader)
		}
		return stats, fmt.Errorf("reading header: %w", err)
	}

	cols, err := mapColumns(header)
	if err != nil {
		return stats, fmt.Errorf("%w: %v", ErrHeader, err)
	}
	// Records must match the header width exactly; shorter or longer
	// records surface as parse errors and are rejected row by row.
	reader.FieldsPerRecord = len(header)

	num := int64(1) // the header itself is record 1
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return stats, nil
		}
		num++
		stats.TotalRows++

		row := Row{Num: num}
		if err != nil {
			row.Reason = fmt.Sprintf("malformed record: %v", err)
			stats.InvalidRows++
		} else {
			row = parseRow(num, cols, record)
			if row.Valid() {
				stats.ValidRows++
			} else {
				stats.InvalidRows++
			}
		}

		if emitErr := emit(row); emitErr != nil {
			return stats, emitErr
		}
	}
}

// parseRow validates and normalizes one well-formed record into a Row.
func parseRow(num int64, cols map[string]int, record []string) Row {
	product, reason := normalize(
		record[cols["merchant_id"]],
		record[cols["product_id"]],
		record[cols["name"]],
		record[cols["price"]],
		record[cols["currency"]],
		record[cols["expiration_date"]],
	)
	return Row{Num: num, Product: product, Reason: reason}
}

// mapColumns resolves the index of every required column from the header.
// Column order and casing are free; duplicate or missing names are rejected.
func mapColumns(header []string) (map[string]int, error) {
	cols := make(map[string]int, len(header))
	for i, name := range header {
		name = strings.ToLower(strings.TrimSpace(name))
		if _, dup := cols[name]; dup {
			return nil, fmt.Errorf("duplicate column %q", name)
		}
		cols[name] = i
	}
	for _, want := range requiredColumns {
		if _, ok := cols[want]; !ok {
			return nil, fmt.Errorf("missing column %q", want)
		}
	}
	return cols, nil
}

// normalize applies the product field rules and returns either the
// normalized product or the first rejection reason.
func normalize(merchantID, productID, name, price, currency, expiration string) (events.ProductData, string) {
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" {
		return events.ProductData{}, "merchant_id is required"
	}
	if len(merchantID) > maxMerchantIDLen {
		return events.ProductData{}, fmt.Sprintf("merchant_id exceeds %d characters", maxMerchantIDLen)
	}

	productID = strings.TrimSpace(productID)
	if productID == "" {
		return events.ProductData{}, "product_id is required"
	}
	if len(productID) > maxProductIDLen {
		return events.ProductData{}, fmt.Sprintf("product_id exceeds %d characters", maxProductIDLen)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return events.ProductData{}, "name is required"
	}
	if len(name) > maxNameLen {
		return events.ProductData{}, fmt.Sprintf("name exceeds %d characters", maxNameLen)
	}

	cents, err := parsePriceCents(strings.TrimSpace(price))
	if err != nil {
		return events.ProductData{}, fmt.Sprintf("price %s", err)
	}

	currency = strings.ToUpper(strings.TrimSpace(currency))
	if _, ok := validCurrencies[currency]; !ok {
		return events.ProductData{}, fmt.Sprintf("currency %q is not supported", currency)
	}

	expiration = strings.TrimSpace(expiration)
	if expiration != "" {
		if _, err := time.Parse(dateLayout, expiration); err != nil {
			return events.ProductData{}, "expiration_date must use the YYYY-MM-DD format"
		}
	}

	return events.ProductData{
		MerchantID:     merchantID,
		ProductID:      productID,
		Name:           name,
		PriceCents:     cents,
		Currency:       currency,
		ExpirationDate: expiration,
	}, ""
}

// parsePriceCents converts a decimal price such as "1499.99" into integer
// cents. Parsing never goes through float64, which cannot represent values
// like 19.99 exactly and would introduce rounding errors. At most two
// fractional digits are accepted.
func parsePriceCents(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("is required")
	}

	intPart, fracPart := s, ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart, fracPart = s[:dot], s[dot+1:]
		if fracPart == "" {
			return 0, errors.New("must have digits after the decimal point")
		}
		switch len(fracPart) {
		case 1:
			fracPart += "0" // "1.5" means 150 cents
		case 2:
		default:
			return 0, errors.New("supports at most two fractional digits")
		}
	}

	if !isDigits(intPart) || (fracPart != "" && !isDigits(fracPart)) {
		return 0, errors.New("must be a decimal number")
	}

	dollars, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil || dollars > maxDollars {
		return 0, errors.New("is out of range")
	}

	var cents int64
	if fracPart != "" {
		if cents, err = strconv.ParseInt(fracPart, 10, 64); err != nil {
			return 0, errors.New("is out of range")
		}
	}

	return dollars*100 + cents, nil
}

// isDigits reports whether s is non-empty ASCII digits only.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// stripBOM consumes a leading UTF-8 byte-order mark if present. Windows
// spreadsheet exports commonly prefix CSV files with one; without stripping
// it the first column name would not be recognized.
func stripBOM(r io.Reader) io.Reader {
	head := make([]byte, len(utf8BOM))
	n, _ := io.ReadFull(r, head)
	if n == len(utf8BOM) && bytes.Equal(head, utf8BOM) {
		return r
	}
	return io.MultiReader(bytes.NewReader(head[:n]), r)
}
