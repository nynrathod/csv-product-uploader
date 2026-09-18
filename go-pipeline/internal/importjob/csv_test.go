package importjob

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

const headerLine = "merchant_id,product_id,name,price,currency,expiration_date"

func TestParsePriceCents(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"1", 100, true},
		{"19.99", 1999, true},
		{"0.01", 1, true},
		{"0.1", 10, true},
		{"123456.78", 12345678, true},
		{"007.50", 750, true},
		{"92233720368547757.99", 9223372036854775799, true},
		{"", 0, false},
		{".5", 0, false},
		{"5.", 0, false},
		{"1.234", 0, false},
		{"abc", 0, false},
		{"1,000.00", 0, false},
		{"-1.00", 0, false},
		{"+1.00", 0, false},
		{"1e3", 0, false},
		{"92233720368547758.00", 0, false},
		{"99999999999999999999", 0, false},
	}

	for _, tc := range cases {
		got, err := parsePriceCents(tc.in)
		if tc.ok && err != nil {
			t.Errorf("parsePriceCents(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if !tc.ok && err == nil {
			t.Errorf("parsePriceCents(%q) expected error, got %d", tc.in, got)
			continue
		}
		if tc.ok && got != tc.want {
			t.Errorf("parsePriceCents(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc        string
		merchantID  string
		productID   string
		productName string
		price       string
		currency    string
		expiration  string
		want        events.ProductData
		reason      string
	}{
		{
			desc:        "valid with expiration",
			merchantID:  " M-1 ",
			productID:   "P-1",
			productName: "  Ergonomic Chair  ",
			price:       "129.99",
			currency:    "usd",
			expiration:  " 2027-03-31 ",
			want: events.ProductData{
				MerchantID: "M-1", ProductID: "P-1", Name: "Ergonomic Chair",
				PriceCents: 12999, Currency: "USD", ExpirationDate: "2027-03-31",
			},
		},
		{
			desc:        "valid without expiration",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: "Desk Lamp",
			price:       "45.00",
			currency:    "EUR",
			want: events.ProductData{
				MerchantID: "M-1", ProductID: "P-1", Name: "Desk Lamp",
				PriceCents: 4500, Currency: "EUR",
			},
		},
		{
			desc:        "missing merchant",
			merchantID:  " ",
			productID:   "P-1",
			productName: "X",
			price:       "1.00",
			currency:    "USD",
			reason:      "merchant_id is required",
		},
		{
			desc:        "merchant too long",
			merchantID:  strings.Repeat("m", 65),
			productID:   "P-1",
			productName: "X",
			price:       "1.00",
			currency:    "USD",
			reason:      "merchant_id exceeds 64 characters",
		},
		{
			desc:        "missing product",
			merchantID:  "M-1",
			productID:   "",
			productName: "X",
			price:       "1.00",
			currency:    "USD",
			reason:      "product_id is required",
		},
		{
			desc:        "missing name",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: "",
			price:       "1.00",
			currency:    "USD",
			reason:      "name is required",
		},
		{
			desc:        "name too long",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: strings.Repeat("x", 257),
			price:       "1.00",
			currency:    "USD",
			reason:      "name exceeds 256 characters",
		},
		{
			desc:        "invalid price",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: "X",
			price:       "abc",
			currency:    "USD",
			reason:      "price must be a decimal number",
		},
		{
			desc:        "unsupported currency",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: "X",
			price:       "1.00",
			currency:    "XYZ",
			reason:      `currency "XYZ" is not supported`,
		},
		{
			desc:        "malformed expiration",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: "X",
			price:       "1.00",
			currency:    "USD",
			expiration:  "31-12-2027",
			reason:      "expiration_date must use the YYYY-MM-DD format",
		},
		{
			desc:        "impossible date",
			merchantID:  "M-1",
			productID:   "P-1",
			productName: "X",
			price:       "1.00",
			currency:    "USD",
			expiration:  "2027-02-31",
			reason:      "expiration_date must use the YYYY-MM-DD format",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got, reason := normalize(tc.merchantID, tc.productID, tc.productName, tc.price, tc.currency, tc.expiration)
			if reason != tc.reason {
				t.Fatalf("reason = %q, want %q", reason, tc.reason)
			}
			if reason == "" && got != tc.want {
				t.Fatalf("product = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestMapColumns(t *testing.T) {
	t.Parallel()

	cols, err := mapColumns([]string{" Name ", "PRICE", "merchant_id", "product_id", "currency", "expiration_date"})
	if err != nil {
		t.Fatalf("mapColumns: %v", err)
	}
	if cols["name"] != 0 || cols["price"] != 1 || cols["merchant_id"] != 2 {
		t.Fatalf("mapping = %+v", cols)
	}
	if _, err := mapColumns([]string{"name", "price"}); err == nil {
		t.Fatal("expected error for missing columns")
	}
	if _, err := mapColumns([]string{"name", "name", "price", "merchant_id", "product_id", "currency", "expiration_date"}); err == nil {
		t.Fatal("expected error for duplicate column")
	}
}

func TestStreamMixedRows(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		"product_id,merchant_id,name,price,currency,expiration_date", // column order is free
		"p-1,m-1,Wireless Mouse,19.99,USD,2027-06-30",
		"p-2,m-1,,19.99,USD,2027-06-30",
		"p-3,m-1,Keyboard,19.999,USD,",
		"p-4,m-1,Monitor,299.00,usd,",
		"p-5,m-1,Cable,5.00,USD,2027-02-31",
		"p-6,m-1,USB Hub,abc,USD,",
	}, "\n")

	var rows []Row
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), func(r Row) error {
		rows = append(rows, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	wantStats := Stats{TotalRows: 6, ValidRows: 2, InvalidRows: 4}
	if stats != wantStats {
		t.Fatalf("stats = %+v, want %+v", stats, wantStats)
	}
	if len(rows) != 6 {
		t.Fatalf("emitted %d rows, want 6", len(rows))
	}
	if rows[0].Num != 2 {
		t.Errorf("first data record numbered %d, want 2 (the header is record 1)", rows[0].Num)
	}

	first := rows[0]
	if !first.Valid() || first.Product != (events.ProductData{
		MerchantID: "m-1", ProductID: "p-1", Name: "Wireless Mouse",
		PriceCents: 1999, Currency: "USD", ExpirationDate: "2027-06-30",
	}) {
		t.Fatalf("first row = %+v", first)
	}

	fourth := rows[3]
	if !fourth.Valid() || fourth.Product.Currency != "USD" || fourth.Product.PriceCents != 29900 || fourth.Product.ExpirationDate != "" {
		t.Fatalf("fourth row = %+v, want normalized lowercase currency and absent expiration", fourth)
	}

	if rows[1].Reason != "name is required" {
		t.Errorf("row 2 reason = %q", rows[1].Reason)
	}
	if rows[2].Reason != "price supports at most two fractional digits" {
		t.Errorf("row 3 reason = %q", rows[2].Reason)
	}
	if !strings.Contains(rows[4].Reason, "expiration_date") {
		t.Errorf("row 5 reason = %q", rows[4].Reason)
	}
	if !strings.Contains(rows[5].Reason, "price") {
		t.Errorf("row 6 reason = %q", rows[5].Reason)
	}
}

func TestStreamRejectsBadHeaders(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc string
		csv  string
	}{
		{"missing column", "merchant_id,product_id,name,price,currency\n"},
		{"duplicate column", "merchant_id,merchant_id,product_id,name,price,currency,expiration_date\n"},
		{"empty file", ""},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := NewStreamer().Stream(context.Background(), strings.NewReader(tc.csv), func(Row) error { return nil })
			if !errors.Is(err, ErrHeader) {
				t.Fatalf("err = %v, want ErrHeader", err)
			}
		})
	}
}

func TestStreamHeaderOnly(t *testing.T) {
	t.Parallel()

	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(headerLine+"\n"), func(Row) error { return nil })
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stats != (Stats{}) {
		t.Fatalf("stats = %+v, want empty stats", stats)
	}
}

func TestStreamIgnoresExtraColumns(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		"Merchant_ID,product_id,name,price,currency,expiration_date,sku",
		"m-1,p-1,Widget,10.00,USD,,SKU-1",
	}, "\n")

	var valid int
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), func(r Row) error {
		if r.Valid() {
			valid++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stats.ValidRows != 1 || valid != 1 {
		t.Fatalf("stats = %+v, valid = %d, want one valid row", stats, valid)
	}
}

func TestStreamStripsUTF8BOM(t *testing.T) {
	t.Parallel()

	src := "\ufeff" + headerLine + "\nm-1,p-1,Widget,10.00,USD,\n"
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), func(Row) error { return nil })
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stats.ValidRows != 1 {
		t.Fatalf("valid = %d, want 1", stats.ValidRows)
	}
}

func TestStreamContinuesPastMalformedRecords(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		headerLine,
		"m-1,p-1",                          // wrong field count
		`m-1,p-2,Widget "lite",10.00,USD,`, // bare quote in an unquoted field
		"m-1,p-3,Widget,10.00,USD,",
	}, "\n")

	var rows []Row
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), func(r Row) error {
		rows = append(rows, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	wantStats := Stats{TotalRows: 3, ValidRows: 1, InvalidRows: 2}
	if stats != wantStats {
		t.Fatalf("stats = %+v, want %+v", stats, wantStats)
	}
	if rows[0].Reason == "" || rows[1].Reason == "" {
		t.Fatalf("expected rejection reasons, got %+v and %+v", rows[0], rows[1])
	}
	if !rows[2].Valid() || rows[2].Product.ProductID != "p-3" {
		t.Fatalf("last row = %+v, want valid p-3", rows[2])
	}
}

func TestStreamStopsOnEmitError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("sink closed")
	src := headerLine + "\n" + strings.Repeat("m-1,p-1,Widget,10.00,USD,\n", 5)

	var emitted int
	_, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), func(Row) error {
		emitted++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if emitted != 1 {
		t.Fatalf("emitted %d rows, want 1", emitted)
	}
}

func TestStreamHonorsContextCancellation(t *testing.T) {
	src := headerLine + "\n" + strings.Repeat("m-1,p-1,Widget,10.00,USD,\n", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var emitted int
	_, err := NewStreamer().Stream(ctx, strings.NewReader(src), func(Row) error {
		emitted++
		if emitted == 3 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if emitted != 3 {
		t.Fatalf("emitted %d rows, want 3", emitted)
	}
}

func TestStreamConsumesUnbufferedSource(t *testing.T) {
	// io.Pipe performs no internal buffering. A parser that tried to hold the
	// whole file, or even large slices of it, before processing would
	// deadlock against this writer. Passing this test is direct evidence of
	// record-at-a-time streaming and therefore of bounded memory.
	const rows = 100_000

	pr, pw := io.Pipe()
	go func() {
		w := bufio.NewWriter(pw)
		fmt.Fprintln(w, headerLine)
		for i := 0; i < rows; i++ {
			fmt.Fprintf(w, "m-%d,p-%d,Widget %d,%d.%02d,USD,2027-01-31\n", i%50, i, i, i/100, i%100)
		}
		w.Flush()
		pw.Close()
	}()

	stats, err := NewStreamer().Stream(context.Background(), pr, func(Row) error { return nil })
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stats.TotalRows != rows || stats.ValidRows != rows || stats.InvalidRows != 0 {
		t.Fatalf("stats = %+v, want %d valid rows", stats, rows)
	}
}
