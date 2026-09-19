// Package importjob implements the CSV import use-cases of the importer
// service: streaming bounded-memory parsing of uploaded product files,
// import job tracking, and publication of product events.
package importjob

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// ErrHeader reports a CSV file whose header row cannot be mapped onto the
// product schema: missing or duplicate columns, an unsupported delimiter,
// or an empty file.
var ErrHeader = errors.New("invalid csv header")

// dateLayout is the canonical expiration format emitted by the parser.
const dateLayout = "2006-01-02"

// dateLayouts lists the accepted input formats; the first is canonical, the
// second is the M/D/YYYY form point-of-sale exports commonly use.
var dateLayouts = []string{"2006-01-02", "1/2/2006"}

// requiredColumns lists the columns the importer cannot work without.
// Identity and currency columns are optional: point-of-sale exports carry
// only name, price and expiration.
var requiredColumns = []string{"name", "price"}

const (
	maxMerchantIDLen = 64
	maxProductIDLen  = 64
	maxNameLen       = 256
	maxHeaderBytes   = 1 << 20
)

// maxDollars is the largest whole-dollar amount whose cent representation
// still fits in an int64.
const maxDollars = (1<<63 - 1 - 99) / 100

// validCurrencies restricts prices to the currencies the platform settles in.
var validCurrencies = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "JPY": {},
	"INR": {}, "AUD": {}, "CAD": {}, "CHF": {}, "CNY": {},
}

// currencySymbols are stripped from price fields such as "$115.55".
const currencySymbols = "$€£¥₹"

// skuPattern extracts the numeric article identifier that product names
// embed as "#(4026987913289674)".
var skuPattern = regexp.MustCompile(`#\((\d+)\)`)

// DefaultMerchantID is applied to files that carry no merchant_id column
// and to uploads that do not name a merchant.
const DefaultMerchantID = "default"

// DefaultCurrency is applied to files that carry no currency column; the
// point-of-sale exports the platform ingests are USD-priced.
const DefaultCurrency = "USD"

// utf8BOM is the byte-order mark prefix produced by Windows spreadsheet
// exports.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// Row is the outcome of validating and normalizing one CSV record. Reason
// is empty when the row is valid and carries the rejection cause otherwise.
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

// StreamOptions supplies fallback values for columns the uploaded file may
// omit. Product files exported from point-of-sale systems frequently carry
// only name, price and expiration columns; identity and currency are then
// taken from these defaults.
type StreamOptions struct {
	DefaultMerchantID string
	DefaultCurrency   string
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
func (s *Streamer) Stream(ctx context.Context, src io.Reader, opts StreamOptions, emit func(Row) error) (Stats, error) {
	var stats Stats

	buffered := bufio.NewReader(stripBOM(src))
	headerLine, err := buffered.ReadBytes('\n')
	if len(headerLine) == 0 && err != nil {
		if errors.Is(err, io.EOF) {
			return stats, fmt.Errorf("%w: file has no header", ErrHeader)
		}
		return stats, fmt.Errorf("reading header: %w", err)
	}
	if len(headerLine) > maxHeaderBytes {
		return stats, fmt.Errorf("%w: header exceeds %d bytes", ErrHeader, maxHeaderBytes)
	}

	// Padded exports end header and data lines with trailing tabs or spaces.
	// That padding must not take part in delimiter detection: a header like
	// "name;price;expiration" followed by padding would otherwise be
	// misread as tab-delimited.
	headerText := strings.TrimRight(strings.TrimRight(string(headerLine), "\r\n"), " \t")
	delim, err := detectDelimiter(headerText)
	if err != nil {
		return stats, fmt.Errorf("%w: %v", ErrHeader, err)
	}

	reader := csv.NewReader(io.MultiReader(bytes.NewReader(headerLine), buffered))
	// Point-of-sale exports are semicolon-delimited; the delimiter is
	// detected from the header line itself.
	reader.Comma = delim
	// ReuseRecord recycles the record buffer between reads, which keeps the
	// steady-state allocation flat while streaming large files.
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		return stats, fmt.Errorf("%w: %v", ErrHeader, err)
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

		var row Row
		if err != nil {
			row = Row{Num: num, Reason: fmt.Sprintf("malformed record: %v", err)}
			stats.InvalidRows++
		} else {
			row = parseRow(num, cols, record, opts)
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

// parseRow validates and normalizes one well-formed record into a Row,
// applying stream defaults for columns the file does not carry.
func parseRow(num int64, cols map[string]int, record []string, opts StreamOptions) Row {
	merchantID, hasMerchant := column(record, cols, "merchant_id")
	if !hasMerchant {
		merchantID = opts.DefaultMerchantID
	}
	// A merchant column that exists but is empty is a data error, not a
	// missing column: normalize() rejects it.
	productID, _ := column(record, cols, "product_id")
	currency, _ := column(record, cols, "currency")
	if strings.TrimSpace(currency) == "" {
		// A missing currency column and a blank currency cell both mean
		// unspecified: the import's default applies.
		currency = opts.DefaultCurrency
	}
	name, _ := column(record, cols, "name")
	price, _ := column(record, cols, "price")
	expiration, _ := column(record, cols, "expiration_date")

	product, reason := normalize(merchantID, productID, name, price, currency, expiration)
	return Row{Num: num, Product: product, Reason: reason}
}

// column returns the field value for the named column and whether the file
// carries that column at all.
func column(record []string, cols map[string]int, name string) (string, bool) {
	idx, ok := cols[name]
	if !ok {
		return "", false
	}
	return record[idx], true
}

// columnAliases maps exported header spellings onto the canonical column
// names the importer understands; point-of-sale exports label the
// expiration column "expiration".
var columnAliases = map[string]string{
	"expiration": "expiration_date",
}

// mapColumns resolves the index of every known column from the header.
// Column order and casing are free; duplicate or missing required names are
// rejected; unknown extra columns are ignored.
func mapColumns(header []string) (map[string]int, error) {
	cols := make(map[string]int, len(header))
	for i, name := range header {
		name = strings.ToLower(strings.TrimSpace(name))
		if canonical, ok := columnAliases[name]; ok {
			name = canonical
		}
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

// detectDelimiter picks ';', ',' or tab by counting occurrences in the
// header outside quoted segments, so both spreadsheet dialects work without
// configuration.
func detectDelimiter(header string) (rune, error) {
	var comma, semicolon, tab int
	inQuotes := false
	for _, r := range header {
		switch r {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				comma++
			}
		case ';':
			if !inQuotes {
				semicolon++
			}
		case '\t':
			if !inQuotes {
				tab++
			}
		}
	}
	if comma == 0 && semicolon == 0 && tab == 0 {
		return 0, errors.New("no supported delimiter (comma, semicolon or tab) found")
	}
	switch {
	case semicolon > comma && semicolon > tab:
		return ';', nil
	case tab > comma && tab > semicolon:
		return '\t', nil
	default:
		return ',', nil
	}
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

	name = strings.TrimSpace(name)
	if name == "" {
		return events.ProductData{}, "name is required"
	}
	if len(name) > maxNameLen {
		return events.ProductData{}, fmt.Sprintf("name exceeds %d characters", maxNameLen)
	}

	productID = strings.TrimSpace(productID)
	if productID == "" {
		productID = deriveProductID(name)
	}
	if len(productID) > maxProductIDLen {
		return events.ProductData{}, fmt.Sprintf("product_id exceeds %d characters", maxProductIDLen)
	}

	cents, err := parsePriceCents(price)
	if err != nil {
		return events.ProductData{}, fmt.Sprintf("price %s", err)
	}

	currency = strings.ToUpper(strings.TrimSpace(currency))
	if currency == "" {
		// Blank means unspecified, not invalid: the platform default
		// applies. Only explicitly unsupported currencies are rejected.
		currency = DefaultCurrency
	}
	if _, ok := validCurrencies[currency]; !ok {
		return events.ProductData{}, fmt.Sprintf("currency %q is not supported", currency)
	}

	expiration, err = parseExpiration(expiration)
	if err != nil {
		return events.ProductData{}, "expiration_date must use YYYY-MM-DD or M/D/YYYY"
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

// deriveProductID builds a stable product identifier for rows without an
// explicit product_id column: the embedded SKU when present, otherwise a
// hash of the product name. Deterministic derivation keeps re-uploads and
// event replays idempotent: the same input always maps to the same product.
func deriveProductID(name string) string {
	if m := skuPattern.FindStringSubmatch(name); m != nil {
		return m[1]
	}
	h := fnv.New64a()
	h.Write([]byte(name))
	return "p-" + strconv.FormatUint(h.Sum64(), 16)
}

// parsePriceCents converts a decimal price such as "$115.55" into integer
// cents. Parsing never goes through float64, which cannot represent values
// like 19.99 exactly and would introduce rounding errors. At most two
// fractional digits are accepted.
func parsePriceCents(s string) (int64, error) {
	s = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(s), currencySymbols))
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

// parseExpiration normalizes M/D/YYYY input into the canonical YYYY-MM-DD
// form; an empty value means the product has no expiration.
func parseExpiration(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format(dateLayout), nil
		}
	}
	return "", errors.New("unsupported date")
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
