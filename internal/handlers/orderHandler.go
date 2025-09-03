package handlers

import (
	"github.com/gofiber/fiber/v2"
	"github.com/k0haku1/analytics-service/internal/service"
)

type OrderHandler struct {
	analytics *service.AnalyticsService
}

func NewOrderHandler(analytics *service.AnalyticsService) *OrderHandler {
	return &OrderHandler{
		analytics: analytics,
	}
}

func (h *OrderHandler) GetPopularProduct(ctx *fiber.Ctx) error {
	product, err := h.analytics.GetMostPopularProduct(ctx.Context())
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return ctx.JSON(product)
}
