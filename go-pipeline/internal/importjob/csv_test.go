package importjob

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
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
		{"$115.55", 11555, true},
		{"€5.00", 500, true},
		{"£ 9.99", 999, true},
		{"¥1000", 100000, true},
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

func TestDeriveProductID(t *testing.T) {
	t.Parallel()

	if got := deriveProductID("Calypso - Lemonade #(4026987913289674)"); got != "4026987913289674" {
		t.Errorf("sku derivation = %q", got)
	}

	// Hash derivation is deterministic and collision-distinct.
	a := deriveProductID("Widget")
	if a != deriveProductID("Widget") {
		t.Fatal("hash derivation is not deterministic")
	}
	if a == deriveProductID("Gadget") {
		t.Fatal("distinct names derived the same id")
	}
	if !strings.HasPrefix(a, "p-") {
		t.Errorf("hash id = %q, want p- prefix", a)
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc       string
		merchantID string
		productID  string
		name       string
		price      string
		currency   string
		expiration string
		want       events.ProductData
		reason     string
	}{
		{
			desc:       "valid with expiration",
			merchantID: "M-1", productID: "P-1", name: "Ergonomic Chair",
			price: "129.99", currency: "usd", expiration: "2027-03-31",
			want: events.ProductData{
				MerchantID: "M-1", ProductID: "P-1", Name: "Ergonomic Chair",
				PriceCents: 12999, Currency: "USD", ExpirationDate: "2027-03-31",
			},
		},
		{
			desc:       "us date format normalized",
			merchantID: "M-1", productID: "", name: "Cheese - Grana Padano #(3566971102136738)",
			price: "$163.88", currency: "", expiration: "1/14/2023",
			want: events.ProductData{
				MerchantID: "M-1", ProductID: "3566971102136738", Name: "Cheese - Grana Padano #(3566971102136738)",
				PriceCents: 16388, Currency: "USD", ExpirationDate: "2023-01-14",
			},
		},
		{
			desc:       "hash id when name carries no sku",
			merchantID: "M-1", productID: "", name: "Widget",
			price: "10.00", currency: "USD",
			want: events.ProductData{
				MerchantID: "M-1", ProductID: deriveProductID("Widget"), Name: "Widget",
				PriceCents: 1000, Currency: "USD",
			},
		},
		{
			desc:       "missing merchant",
			merchantID: " ", productID: "P-1", name: "X",
			price: "1.00", currency: "USD",
			reason: "merchant_id is required",
		},
		{
			desc:       "merchant too long",
			merchantID: strings.Repeat("m", 65), productID: "P-1", name: "X",
			price: "1.00", currency: "USD",
			reason: "merchant_id exceeds 64 characters",
		},
		{
			desc:       "missing name",
			merchantID: "M-1", productID: "P-1", name: "",
			price: "1.00", currency: "USD",
			reason: "name is required",
		},
		{
			desc:       "name too long",
			merchantID: "M-1", productID: "P-1", name: strings.Repeat("x", 257),
			price: "1.00", currency: "USD",
			reason: "name exceeds 256 characters",
		},
		{
			desc:       "invalid price",
			merchantID: "M-1", productID: "P-1", name: "X",
			price: "abc", currency: "USD",
			reason: "price must be a decimal number",
		},
		{
			desc:       "unsupported currency",
			merchantID: "M-1", productID: "P-1", name: "X",
			price: "1.00", currency: "XYZ",
			reason: `currency "XYZ" is not supported`,
		},
		{
			desc:       "malformed expiration",
			merchantID: "M-1", productID: "P-1", name: "X",
			price: "1.00", currency: "USD", expiration: "31-12-2027",
			reason: "expiration_date must use YYYY-MM-DD or M/D/YYYY",
		},
		{
			desc:       "impossible date",
			merchantID: "M-1", productID: "P-1", name: "X",
			price: "1.00", currency: "USD", expiration: "2027-02-31",
			reason: "expiration_date must use YYYY-MM-DD or M/D/YYYY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got, reason := normalize(tc.merchantID, tc.productID, tc.name, tc.price, tc.currency, tc.expiration)
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
	if _, err := mapColumns([]string{"merchant_id"}); err == nil {
		t.Fatal("expected error for missing required columns")
	}
	if _, err := mapColumns([]string{"name", "name", "price"}); err == nil {
		t.Fatal("expected error for duplicate column")
	}
}

func TestDetectDelimiter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want rune
		ok   bool
	}{
		{"name;price;expiration", ';', true},
		{"name,price,expiration", ',', true},
		{"name\tprice", '\t', true},
		{"name price", 0, false},
	}
	for _, tc := range cases {
		got, err := detectDelimiter(tc.in)
		if tc.ok && (err != nil || got != tc.want) {
			t.Errorf("detectDelimiter(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
		if !tc.ok && err == nil {
			t.Errorf("detectDelimiter(%q) expected error", tc.in)
		}
	}
}

func TestStreamMixedRows(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		"product_id,merchant_id,name,price,currency,expiration_date",
		"p-1,m-1,Wireless Mouse,19.99,USD,2027-06-30",
		"p-2,m-1,,19.99,USD,2027-06-30",
		"p-3,m-1,Keyboard,19.999,USD,",
		"p-4,m-1,Monitor,299.00,usd,",
		"p-5,m-1,Cable,5.00,USD,2027-02-31",
		"p-6,m-1,USB Hub,abc,USD,",
	}, "\n")

	var rows []Row
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), StreamOptions{}, func(r Row) error {
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

func TestStreamSemicolonDialect(t *testing.T) {
	t.Parallel()

	src := strings.Join([]string{
		"name;price;expiration",
		"Calypso - Lemonade #(4026987913289674);$115.55;1/11/2023",
		"Wine - White\t Colubia Cresh #(3572270474512358);$126.90;11/23/2022",
		"Veal - Loin #(5552033378109898);$72.60;12/16/2022",
		"Shrimp - Black Tiger 8 - 12 #(3539841640347903);not-a-price;12/25/2022",
	}, "\n")

	opts := StreamOptions{DefaultMerchantID: "default", DefaultCurrency: "USD"}

	var rows []Row
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), opts, func(r Row) error {
		rows = append(rows, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	wantStats := Stats{TotalRows: 4, ValidRows: 3, InvalidRows: 1}
	if stats != wantStats {
		t.Fatalf("stats = %+v, want %+v", stats, wantStats)
	}

	first := rows[0]
	if !first.Valid() || first.Product != (events.ProductData{
		MerchantID: "default", ProductID: "4026987913289674",
		Name:       "Calypso - Lemonade #(4026987913289674)",
		PriceCents: 11555, Currency: "USD", ExpirationDate: "2023-01-11",
	}) {
		t.Fatalf("first row = %+v", first)
	}

	if rows[1].Product.ProductID != "3572270474512358" {
		t.Errorf("second row id = %q, want embedded sku", rows[1].Product.ProductID)
	}
	if rows[2].Product.ExpirationDate != "2022-12-16" {
		t.Errorf("third row date = %q", rows[2].Product.ExpirationDate)
	}
	if rows[3].Valid() || !strings.Contains(rows[3].Reason, "price") {
		t.Errorf("fourth row = %+v, want price rejection", rows[3])
	}
}

func TestStreamRejectsBadHeaders(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc string
		csv  string
	}{
		{"missing column", "merchant_id,product_id\n"},
		{"duplicate column", "merchant_id,merchant_id,name,price\n"},
		{"empty file", ""},
		{"no delimiter", "name price expiration\n"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := NewStreamer().Stream(context.Background(), strings.NewReader(tc.csv), StreamOptions{}, func(Row) error { return nil })
			if !errors.Is(err, ErrHeader) {
				t.Fatalf("err = %v, want ErrHeader", err)
			}
		})
	}
}

func TestStreamHeaderOnly(t *testing.T) {
	t.Parallel()

	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(headerLine+"\n"), StreamOptions{}, func(Row) error { return nil })
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
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), StreamOptions{}, func(r Row) error {
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
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), StreamOptions{}, func(Row) error { return nil })
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
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), StreamOptions{}, func(r Row) error {
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
	_, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), StreamOptions{}, func(Row) error {
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
	_, err := NewStreamer().Stream(ctx, strings.NewReader(src), StreamOptions{}, func(Row) error {
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

	stats, err := NewStreamer().Stream(context.Background(), pr, StreamOptions{}, func(Row) error { return nil })
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stats.TotalRows != rows || stats.ValidRows != rows || stats.InvalidRows != 0 {
		t.Fatalf("stats = %+v, want %d valid rows", stats, rows)
	}
}

func TestStreamPaddedSemicolonExport(t *testing.T) {
	t.Parallel()

	// Point-of-sale exports pad lines with trailing tabs; parsing must see
	// past the padding, including during delimiter detection.
	src := "name;price;expiration\t\t\t\n" +
		"Calypso - Lemonade #(4026987913289674);$115.55;1/11/2023\t\t\n" +
		"Veal - Loin #(5552033378109898);$72.60;12/16/2022\t\n"

	opts := StreamOptions{DefaultMerchantID: "default", DefaultCurrency: "USD"}

	var rows []Row
	stats, err := NewStreamer().Stream(context.Background(), strings.NewReader(src), opts, func(r Row) error {
		rows = append(rows, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stats.TotalRows != 2 || stats.ValidRows != 2 || stats.InvalidRows != 0 {
		t.Fatalf("stats = %+v, want 2 valid rows", stats)
	}
	if !rows[0].Valid() || rows[0].Product.ExpirationDate != "2023-01-11" {
		t.Fatalf("first row = %+v, want expiration 2023-01-11", rows[0])
	}
	if rows[1].Product.ProductID != "5552033378109898" {
		t.Fatalf("second row id = %q, want embedded sku", rows[1].Product.ProductID)
	}
}
