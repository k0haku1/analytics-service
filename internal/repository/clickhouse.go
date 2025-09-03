package repository

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/k0haku1/analytics-service/internal/models"
	"log"
)

type ClickHouseRepository struct {
	conn clickhouse.Conn
}

func NewClickHouseRepository(conn clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{conn: conn}
}

func (r *ClickHouseRepository) Insert(ctx context.Context, orderID uuid.UUID, customerID uuid.UUID, productID uuid.UUID, productName string, quantity int64, eventKey string) error {

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO analytics.orders (order_id, customer_id, product_id, product_name, quantity, event_key)")
	if err != nil {
		log.Printf("Error preparing batch:", err)
		return err
	}

	if err := batch.Append(orderID, customerID, productID, productName, quantity, eventKey); err != nil {
		log.Printf("Error appending to batch:", err)
		return err
	}

	if err := batch.Send(); err != nil {
		log.Printf("Error sending batch to ClickHouse:", err)
		return err
	}

	fmt.Println("Batch sent successfully")
	return nil
}

func (r *ClickHouseRepository) GetMostPopular(ctx context.Context) (*models.Product, error) {
	var product models.Product
	query := `
        SELECT product_id, product_name, SUM(quantity) AS total
        FROM analytics.orders
        GROUP BY product_id, product_name
        ORDER BY total DESC
        LIMIT 1
        `
	if err := r.conn.QueryRow(ctx, query).Scan(&product.ID, &product.Name, &product.Quantity); err != nil {
		return nil, err
	}
	return &product, nil
}
