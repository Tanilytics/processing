package storage

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnOpenStrategy(t *testing.T) {
	strategy, err := parseConnOpenStrategy("in_order")
	require.NoError(t, err)
	assert.Equal(t, clickhouse.ConnOpenInOrder, strategy)

	strategy, err = parseConnOpenStrategy("round_robin")
	require.NoError(t, err)
	assert.Equal(t, clickhouse.ConnOpenRoundRobin, strategy)

	strategy, err = parseConnOpenStrategy("random")
	require.NoError(t, err)
	assert.Equal(t, clickhouse.ConnOpenRandom, strategy)
}

func TestParseConnOpenStrategyRejectsInvalidValue(t *testing.T) {
	strategy, err := parseConnOpenStrategy("weighted")
	require.Error(t, err)
	assert.Equal(t, clickhouse.ConnOpenStrategy(0), strategy)
	assert.ErrorContains(t, err, "must be in_order, round_robin, or random")
}

func TestNewClickHouseWriterRejectsInvalidOptions(t *testing.T) {
	writer, err := NewClickHouseWriter(Options{
		Database:         "default",
		Username:         "default",
		Password:         "change-me",
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnOpenStrategy: "in_order",
	})
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.ErrorContains(t, err, "clickhouse addrs must not be empty")

	writer, err = NewClickHouseWriter(Options{
		Addrs:            []string{"clickhouse:9000"},
		Database:         "default",
		Username:         "default",
		Password:         "change-me",
		MaxOpenConns:     0,
		MaxIdleConns:     5,
		ConnOpenStrategy: "in_order",
	})
	require.Error(t, err)
	assert.Nil(t, writer)
	assert.ErrorContains(t, err, "clickhouse max open conns must be > 0")
}
