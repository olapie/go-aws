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

type DDBAttributeValuer interface {
	AttributeValue() map[string]*dynamodb.AttributeValue
}

type DDBKeyTypeSet interface {
	~string | constraints.Signed | constraints.Unsigned
}

type DDBPrimaryKey[P DDBKeyTypeSet, R DDBKeyTypeSet] struct {
	PartitionName string
	RangeName     string
}

func (pk *DDBPrimaryKey[P, R]) AttributeValue(p P, r *R) map[string]*dynamodb.AttributeValue {
	attrs := make(map[string]*dynamodb.AttributeValue)
	if str, ok := any(p).(string); ok {
		attrs[pk.PartitionName] = &dynamodb.AttributeValue{
			S: aws.String(str),
		}
	} else {
		attrs[pk.PartitionName] = &dynamodb.AttributeValue{
			N: aws.String(fmt.Sprint(p)),
		}
	}

	if r != nil {
		if pk.RangeName == "" {
			panic("no range key specified")
		}

		if str, ok := any(r).(string); ok {
			attrs[pk.PartitionName] = &dynamodb.AttributeValue{
				S: aws.String(str),
			}
		} else {
			attrs[pk.PartitionName] = &dynamodb.AttributeValue{
				N: aws.String(fmt.Sprint(r)),
			}
		}
	}

	return attrs
}

type DDBTable[T any, P DDBKeyTypeSet, R DDBKeyTypeSet] struct {
	db         *dynamodb.DynamoDB
	tableName  string
	primaryKey *DDBPrimaryKey[P, R]
	columns    []string
}

func NewDDBTable[T any, P DDBKeyTypeSet, R DDBKeyTypeSet](db *dynamodb.DynamoDB, tableName string, primaryKey *DDBPrimaryKey[P, R], columns []string) *DDBTable[T, P, R] {
	return &DDBTable[T, P, R]{
		db:         db,
		tableName:  tableName,
		primaryKey: primaryKey,
		columns:    columns,
	}
}

func (r *DDBTable[T, P, R]) Save(ctx context.Context, item T) error {
	attrs, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	input := &dynamodb.PutItemInput{
		Item:                   attrs,
		ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName:              aws.String(r.tableName),
	}
	_, err = r.db.PutItemWithContext(ctx, input)
	return errors.Wrapf(err, "save item")
}

func (r *DDBTable[T, P, R]) BatchSave(ctx context.Context, items []T) error {
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
		r.tableName: requests,
	}}
	_, err := r.db.BatchWriteItemWithContext(ctx, input)
	return errors.Wrapf(err, "batch write item")
}

func (r *DDBTable[T, P, R]) Get(ctx context.Context, partitionKey P, rangeKey *R) (T, error) {
	input := &dynamodb.GetItemInput{
		Key:       r.primaryKey.AttributeValue(partitionKey, rangeKey),
		TableName: aws.String(r.tableName),
	}
	var item T
	output, err := r.db.GetItemWithContext(ctx, input)
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

func (r *DDBTable[T, P, R]) Delete(ctx context.Context, partitionKey P, rangeKey *R) error {
	input := &dynamodb.DeleteItemInput{
		Key:       r.primaryKey.AttributeValue(partitionKey, rangeKey),
		TableName: aws.String(r.tableName),
	}
	_, err := r.db.DeleteItemWithContext(ctx, input)
	return errors.Wrapf(err, "cannot delete item from db")
}

func (r *DDBTable[T, P, R]) BatchDelete(ctx context.Context, partitionKey P, rangeKeys ...R) error {
	requests := make([]*dynamodb.WriteRequest, len(rangeKeys))
	for i, rk := range rangeKeys {
		requests[i] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: r.primaryKey.AttributeValue(partitionKey, &rk),
			},
		}
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{
		r.tableName: requests,
	}}
	_, err := r.db.BatchWriteItemWithContext(ctx, input)
	return errors.Wrapf(err, "batch write item")
}

func (r *DDBTable[T, P, R]) Query(ctx context.Context, partitionKey P) ([]T, error) {
	keyCond := expression.Key(r.primaryKey.PartitionName).Equal(expression.Value(partitionKey))
	var cols []expression.NameBuilder
	for _, col := range r.columns {
		cols = append(cols, expression.Name(col))
	}
	proj := expression.NamesList(cols[0], cols[1:]...)
	expr, err := expression.NewBuilder().
		WithKeyCondition(keyCond).
		WithProjection(proj).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build expression: %w", err)
	}
	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(r.tableName),
	}
	result, err := r.db.QueryWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var items []T
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return items, nil
}
