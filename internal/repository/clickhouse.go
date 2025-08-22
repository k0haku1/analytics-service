package repository

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type ClickHouseRepository struct {
	conn clickhouse.Conn
}

func NewClickHouseRepository(conn clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{conn: conn}
}

func (r *ClickHouseRepository) Insert(ctx context.Context, orderID uuid.UUID, customerID uuid.UUID, productID uuid.UUID, productName string, quantity int32, eventKey string) error {

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO analytics.orders (order_id, customer_id, product_id, product_name, quantity, event_key)")
	if err != nil {
		fmt.Println("Error preparing batch:", err)
		return err
	}

	if err := batch.Append(orderID, customerID, productID, productName, quantity, eventKey); err != nil {
		fmt.Println("Error appending to batch:", err)
		return err
	}

	if err := batch.Send(); err != nil {
		fmt.Println("Error sending batch to ClickHouse:", err)
		return err
	}

	fmt.Println("Batch sent successfully")
	return nil
}
