package awskit

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DDBIndex[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint] struct {
	table *DDBTable[E, P, S]
}

func NewDDBIndex[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint](
	db *dynamodb.Client,
	tableName string,
	indexName string,
	pk *DDBPrimaryKey[P, S],
	columns []string,
) *DDBIndex[E, P, S] {
	i := &DDBIndex[E, P, S]{
		table: NewDDBTable[E, P, S](db, tableName, pk, columns),
	}
	i.table.indexName = &indexName
	return i
}

func (i *DDBIndex[E, P, S]) Query(ctx context.Context, partition P, options ...func(input *dynamodb.QueryInput)) ([]E, error) {
	return i.table.Query(ctx, partition, options...)
}

func (i *DDBIndex[E, P, S]) QueryPage(ctx context.Context, partition P, startToken string, limit int, options ...func(input *dynamodb.QueryInput)) (items []E, nextToken string, err error) {
	return i.table.QueryPage(ctx, partition, startToken, limit, options...)
}

func (i *DDBIndex[E, P, S]) QueryFirstOne(ctx context.Context, partition P) (item E, err error) {
	return i.table.QueryFirstOne(ctx, partition)
}

func (i *DDBIndex[E, P, S]) QueryLastOne(ctx context.Context, partition P) (item E, err error) {
	return i.table.QueryLastOne(ctx, partition)
}
