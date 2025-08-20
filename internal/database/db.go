package database

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"os"
	"time"
)

func NewClickhouseConn() (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: os.Getenv("CLICKHOUSE_DATABASE"),
			Username: os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASS"),
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}
