package awskit

import (
	"context"
	"fmt"
	"golang.org/x/exp/constraints"

	"code.olapie.com/errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
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
	RangeKey     string
}

func (pk *DDBPrimaryKey[P, S]) AttributeValue(p P, s S) map[string]*dynamodb.AttributeValue {
	attrs := make(map[string]*dynamodb.AttributeValue)
	if str, ok := any(p).(string); ok {
		attrs[pk.PartitionKey] = &dynamodb.AttributeValue{
			S: aws.String(str),
		}
	} else {
		attrs[pk.PartitionKey] = &dynamodb.AttributeValue{
			N: aws.String(fmt.Sprint(p)),
		}
	}

	if _, ok := any(s).(DDBNoSortKey); ok {
		return attrs
	}

	if pk.RangeKey == "" {
		panic("no range key specified")
	}

	if str, ok := any(s).(string); ok {
		attrs[pk.RangeKey] = &dynamodb.AttributeValue{
			S: aws.String(str),
		}
	} else {
		attrs[pk.RangeKey] = &dynamodb.AttributeValue{
			N: aws.String(fmt.Sprint(s)),
		}
	}

	return attrs
}

// DDBTable is a wrapper of dynamodb table providing helpful operations
// E - type of item
// P - type of partition key
// S - type of sort key
type DDBTable[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint] struct {
	db         *dynamodb.DynamoDB
	tableName  string
	primaryKey *DDBPrimaryKey[P, S]
	columns    []string
}

func NewDDBTable[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint](db *dynamodb.DynamoDB, tableName string, pk *DDBPrimaryKey[P, S], columns []string) *DDBTable[E, P, S] {
	return &DDBTable[E, P, S]{
		db:         db,
		tableName:  tableName,
		primaryKey: pk,
		columns:    columns,
	}
}

func (t *DDBTable[E, P, S]) Save(ctx context.Context, item E) error {
	attrs, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	input := &dynamodb.PutItemInput{
		Item:                   attrs,
		ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName:              aws.String(t.tableName),
	}
	_, err = t.db.PutItemWithContext(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) BatchSave(ctx context.Context, items []E) error {
	requests := make([]*dynamodb.WriteRequest, len(items))
	for i, item := range items {
		attrs, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		requests[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: attrs,
			},
		}
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{
		t.tableName: requests,
	}}
	_, err := t.db.BatchWriteItemWithContext(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) Get(ctx context.Context, partition P, rangeKey S) (E, error) {
	input := &dynamodb.GetItemInput{
		Key:       t.primaryKey.AttributeValue(partition, rangeKey),
		TableName: aws.String(t.tableName),
	}
	var item E
	output, err := t.db.GetItemWithContext(ctx, input)
	if err != nil {
		return item, fmt.Errorf("get item: %w", err)
	}

	if output.Item == nil {
		return item, errors.NotExist
	}

	err = dynamodbattribute.UnmarshalMap(output.Item, &item)
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
	_, err := t.db.DeleteItemWithContext(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) BatchDelete(ctx context.Context, partition P, sortKeys ...S) error {
	requests := make([]*dynamodb.WriteRequest, len(sortKeys))
	for i, rk := range sortKeys {
		requests[i] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: t.primaryKey.AttributeValue(partition, rk),
			},
		}
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{
		t.tableName: requests,
	}}
	_, err := t.db.BatchWriteItemWithContext(ctx, input)
	return err
}

func (t *DDBTable[E, P, S]) Query(ctx context.Context, partition P) ([]E, error) {
	keyCond := expression.Key(t.primaryKey.PartitionKey).Equal(expression.Value(partition))
	var cols []expression.NameBuilder
	for _, col := range t.columns {
		cols = append(cols, expression.Name(col))
	}
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
	result, err := t.db.QueryWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var items []E
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return items, nil
}
