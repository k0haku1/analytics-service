package database

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"time"
)

func NewClickhouseConn() (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "analytics",
			Username: "default",
			Password: "root",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}
