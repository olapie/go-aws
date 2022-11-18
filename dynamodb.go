package awskit

import (
	"context"
	"fmt"

	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/exp/constraints"
)

// DDBNoSortKey means table doesn't have sort key
type DDBNoSortKey *any

type DDBPartitionKeyConstraint interface {
	~string | constraints.Signed | constraints.Unsigned
}

type DDBSortKeyConstraint interface {
	DDBPartitionKeyConstraint | DDBNoSortKey
}

type DDBPrimaryKey[P DDBPartitionKeyConstraint, S DDBSortKeyConstraint] struct {
	PartitionKey string
	SortKey      string
}

func (pk *DDBPrimaryKey[P, S]) AttributeValue(p P, s S) map[string]types.AttributeValue {
	attrs := make(map[string]types.AttributeValue)
	if str, ok := any(p).(string); ok {
		attrs[pk.PartitionKey] = &types.AttributeValueMemberS{
			Value: str,
		}
	} else {
		attrs[pk.PartitionKey] = &types.AttributeValueMemberN{
			Value: fmt.Sprint(p),
		}
	}

	if _, ok := any(s).(DDBNoSortKey); ok {
		return attrs
	}

	if pk.SortKey == "" {
		panic("sort key is not defined")
	}

	if str, ok := any(s).(string); ok {
		attrs[pk.SortKey] = &types.AttributeValueMemberS{
			Value: str,
		}
	} else {
		attrs[pk.SortKey] = &types.AttributeValueMemberN{
			Value: fmt.Sprint(s),
		}
	}

	return attrs
}

// DDBTable is a wrapper of dynamodb table providing helpful operations
// E - type of item
// P - type of partition key
// S - type of sort key
type DDBTable[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint] struct {
	client     *dynamodb.Client
	tableName  string
	primaryKey *DDBPrimaryKey[P, S]
	columns    []string
}

func NewDDBTable[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint](db *dynamodb.Client, tableName string, pk *DDBPrimaryKey[P, S], columns []string) *DDBTable[E, P, S] {
	return &DDBTable[E, P, S]{
		client:     db,
		tableName:  tableName,
		primaryKey: pk,
		columns:    columns,
	}
}

func (t *DDBTable[E, P, S]) Save(ctx context.Context, item E) error {
	attrs, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	input := &dynamodb.PutItemInput{
		Item:                   attrs,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TableName:              aws.String(t.tableName),
	}
	_, err = t.client.PutItem(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) BatchSave(ctx context.Context, items []E) error {
	requests, err := conv.Slice(items, func(item E) (types.WriteRequest, error) {
		attrs, err := attributevalue.MarshalMap(item)
		if err != nil {
			return types.WriteRequest{}, err
		}
		return types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: attrs,
			},
		}, nil
	})

	if err != nil {
		return fmt.Errorf("cannot create write request: %w", err)
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		t.tableName: requests,
	}}
	_, err = t.client.BatchWriteItem(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) Get(ctx context.Context, partition P, rangeKey S) (E, error) {
	input := &dynamodb.GetItemInput{
		Key:       t.primaryKey.AttributeValue(partition, rangeKey),
		TableName: aws.String(t.tableName),
	}
	var item E
	output, err := t.client.GetItem(ctx, input)
	if err != nil {
		return item, fmt.Errorf("get item: %w", err)
	}

	if output.Item == nil {
		return item, errors.NotExist
	}

	err = attributevalue.UnmarshalMap(output.Item, &item)
	if err != nil {
		return item, fmt.Errorf("unmarshal: %w", err)
	}
	return item, nil
}

func (t *DDBTable[E, P, S]) Delete(ctx context.Context, partition P, rangeKey S) error {
	input := &dynamodb.DeleteItemInput{
		Key:       t.primaryKey.AttributeValue(partition, rangeKey),
		TableName: aws.String(t.tableName),
	}
	_, err := t.client.DeleteItem(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) BatchDelete(ctx context.Context, partition P, sortKeys ...S) error {
	requests := conv.MustSlice(sortKeys, func(k S) types.WriteRequest {
		return types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: t.primaryKey.AttributeValue(partition, k),
			},
		}
	})

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		t.tableName: requests,
	}}
	_, err := t.client.BatchWriteItem(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) Query(ctx context.Context, partition P) ([]E, error) {
	keyCond := expression.Key(t.primaryKey.PartitionKey).Equal(expression.Value(partition))
	cols := conv.MustSlice(t.columns, expression.Name)
	proj := expression.NamesList(cols[0], cols[1:]...)
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("build expression: %w", err)
	}

	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(t.tableName),
	}
	result, err := t.client.Query(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var items []E
	err = attributevalue.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return items, nil
}
