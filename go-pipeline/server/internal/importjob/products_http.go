package importjob

import (
	"context"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
)

// ProductsHandler exposes read-only product queries over the importer's
// own projection. Products live in the catalog-worker's database; this
// endpoint serves the importer's derived view so the upload UI can show
// what an import delivered without any cross-database access.
type ProductsHandler struct {
	store ProjectionStore
}

// NewProductsHandler builds the handler.
func NewProductsHandler(store ProjectionStore) *ProductsHandler {
	return &ProductsHandler{store: store}
}

// Register mounts the products route.
func (h *ProductsHandler) Register(app *fiber.App) {
	app.Get("/api/v1/imports/:id/products", h.list)
}

// productResponse is the external product representation.
type productResponse struct {
	MerchantID     string  `json:"merchantId"`
	ProductID      string  `json:"productId"`
	Name           string  `json:"name"`
	PriceCents     int64   `json:"priceCents"`
	Currency       string  `json:"currency"`
	ExpirationDate *string `json:"expirationDate"`
}

// list returns one page of the products projected from an import, with
// name filtering and column sorting.
func (h *ProductsHandler) list(c fiber.Ctx) error {
	id := c.Params("id")
	if !uuidPattern.MatchString(id) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid import id"})
	}

	page := clampInt(c.Query("page"), 1, 1, 1_000_000)
	limit := clampInt(c.Query("limit"), 20, 1, 100)

	sortBy := c.Query("sortBy")
	switch sortBy {
	case "name", "price", "expiration":
	default:
		sortBy = "name"
	}
	sortOrder := c.Query("sortOrder")
	if sortOrder != "ASC" && sortOrder != "DESC" {
		sortOrder = "ASC"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	products, total, err := h.store.ListProducts(ctx, id, ProductQuery{
		FilterName: c.Query("filterName"),
		SortBy:     sortBy,
		SortOrder:  sortOrder,
		Limit:      limit,
		Offset:     (page - 1) * limit,
	})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "loading products failed"})
	}

	items := make([]productResponse, 0, len(products))
	for _, p := range products {
		var exp *string
		if p.ExpirationDate != "" {
			s := p.ExpirationDate
			exp = &s
		}
		items = append(items, productResponse{
			MerchantID: p.MerchantID, ProductID: p.ProductID, Name: p.Name,
			PriceCents: p.PriceCents, Currency: p.Currency, ExpirationDate: exp,
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(limit) - 1) / int64(limit))
	}

	return c.JSON(fiber.Map{
		"data": items,
		"pagination": fiber.Map{
			"page": page, "limit": limit, "total": total, "totalPages": totalPages,
		},
	})
}

// clampInt parses a query parameter as an integer and clamps it into
// [min, max], falling back to def when absent or malformed.
func clampInt(raw string, def, min, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}
