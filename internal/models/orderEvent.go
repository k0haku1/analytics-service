package models

import "github.com/google/uuid"

type OrderEvent struct {
	OrderID    uuid.UUID `json:"order_id"`
	CustomerID uuid.UUID `json:"customer_id"`
	Products   []Product `json:"products"`
}
