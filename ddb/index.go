package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Index[E any, P PartitionKeyConstraint, S SortKeyConstraint] struct {
	table *Table[E, P, S]
}

func NewIndex[E any, P PartitionKeyConstraint, S SortKeyConstraint](
	db *dynamodb.Client,
	tableName string,
	indexName string,
	pk *PrimaryKeyDefinition[P, S],
) *Index[E, P, S] {
	i := &Index[E, P, S]{
		table: NewTable[E, P, S](db, tableName, pk),
	}
	i.table.indexName = &indexName
	return i
}

func (i *Index[E, P, S]) Query(ctx context.Context, partition P, sortKey *S, options ...func(input *dynamodb.QueryInput)) ([]E, error) {
	return i.table.Query(ctx, partition, sortKey, options...)
}

func (i *Index[E, P, S]) QueryPage(ctx context.Context, partition P, sortKey *S, startToken string, limit int, options ...func(input *dynamodb.QueryInput)) (items []E, nextToken string, err error) {
	return i.table.QueryPage(ctx, partition, sortKey, startToken, limit, options...)
}

func (i *Index[E, P, S]) QueryFirstOne(ctx context.Context, partition P, sortKey *S) (item E, err error) {
	return i.table.QueryFirstOne(ctx, partition, sortKey)
}

func (i *Index[E, P, S]) QueryLastOne(ctx context.Context, partition P, sortKey *S) (item E, err error) {
	return i.table.QueryLastOne(ctx, partition, sortKey)
}
