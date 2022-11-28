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
)

// DDBTable is a wrapper of dynamodb table providing helpful operations
// E - type of item
// P - type of partition key
// S - type of sort key
type DDBTable[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint] struct {
	client     *dynamodb.Client
	tableName  string
	indexName  *string
	primaryKey *DDBPrimaryKey[P, S]
	columns    []string
}

func NewDDBTable[E any, P DDBPartitionKeyConstraint, S DDBSortKeyConstraint](db *dynamodb.Client, tableName string, pk *DDBPrimaryKey[P, S], columns []string) *DDBTable[E, P, S] {
	t := &DDBTable[E, P, S]{
		client:     db,
		tableName:  tableName,
		primaryKey: pk,
		columns:    columns,
	}
	return t
}

func (t *DDBTable[E, P, S]) Save(ctx context.Context, item E) error {
	attrs, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("attributevalue.MarshalMap: %w", err)
	}
	input := &dynamodb.PutItemInput{
		Item:                   attrs,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TableName:              aws.String(t.tableName),
	}
	_, err = t.client.PutItem(ctx, input)
	return errors.Wrapf(err, "dynamodb.PutItem")
}

func (t *DDBTable[E, P, S]) BatchSave(ctx context.Context, items []E) error {
	requests, err := conv.Slice(items, func(item E) (types.WriteRequest, error) {
		var req types.WriteRequest
		attrs, err := attributevalue.MarshalMap(item)
		if err != nil {
			return req, fmt.Errorf("attributevalue.MarshalMap: %w", err)
		}
		req.PutRequest = &types.PutRequest{Item: attrs}
		return req, nil
	})

	if err != nil {
		return fmt.Errorf("convert items to WriteRequest list: %w", err)
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		t.tableName: requests,
	}}
	_, err = t.client.BatchWriteItem(ctx, input)
	return errors.Wrapf(err, "dynamodb.BatchWriteItem")
}

func (t *DDBTable[E, P, S]) Get(ctx context.Context, partition P, sortKey S) (E, error) {
	input := &dynamodb.GetItemInput{
		Key:       t.primaryKey.AttributeValue(partition, sortKey),
		TableName: aws.String(t.tableName),
	}
	var item E
	output, err := t.client.GetItem(ctx, input)
	if err != nil {
		return item, fmt.Errorf("dynamodb.GetItem: %w", err)
	}

	if output.Item == nil {
		return item, errors.NotExist
	}

	err = attributevalue.UnmarshalMap(output.Item, &item)
	if err != nil {
		return item, fmt.Errorf("attributevalue.UnmarshalMap: %w", err)
	}
	return item, nil
}

func (t *DDBTable[E, P, S]) Delete(ctx context.Context, partition P, sortKey S) error {
	input := &dynamodb.DeleteItemInput{
		Key:       t.primaryKey.AttributeValue(partition, sortKey),
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

func (t *DDBTable[E, P, S]) Query(ctx context.Context, partition P, options ...func(input *dynamodb.QueryInput)) ([]E, error) {
	input, err := t.createQueryInput(partition, 1024)
	if err != nil {
		return nil, fmt.Errorf("createQueryInput: %w", err)
	}

	for _, op := range options {
		op(input)
	}

	var items []E
	paginator := dynamodb.NewQueryPaginator(t.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return items, fmt.Errorf("paginator.NextPage: %w", err)
		}
		var pageItems []E
		err = attributevalue.UnmarshalListOfMaps(output.Items, &pageItems)
		if err != nil {
			return nil, fmt.Errorf("attributevalue.UnmarshalListOfMaps: %w", err)
		}
		items = append(items, pageItems...)
	}
	return items, nil
}

func (t *DDBTable[E, P, S]) QueryPage(ctx context.Context, partition P, startToken string, limit int, options ...func(input *dynamodb.QueryInput)) (items []E, nextToken string, err error) {
	input, err := t.createQueryInput(partition, int32(limit))
	if err != nil {
		return nil, nextToken, fmt.Errorf("createQueryInput: %w", err)
	}

	for _, op := range options {
		op(input)
	}

	if startToken != "" {
		input.ExclusiveStartKey, err = t.primaryKey.DecodeAttributeValue(startToken)
		if err != nil {
			return nil, nextToken, fmt.Errorf("decodeStartKey: %w", err)
		}
	}

	output, err := t.client.Query(ctx, input)
	if err != nil {
		return nil, nextToken, fmt.Errorf("dynamodb.Query: %w", err)
	}
	err = attributevalue.UnmarshalListOfMaps(output.Items, &items)
	if err != nil {
		return nil, nextToken, fmt.Errorf("attributevalue.UnmarshalListOfMaps: %w", err)
	}
	if len(output.LastEvaluatedKey) == 0 {
		nextToken = ""
	} else {
		nextToken = t.primaryKey.EncodeAttributeValue(output.LastEvaluatedKey)
	}
	return items, nextToken, nil
}

func (t *DDBTable[E, P, S]) QueryFirstOne(ctx context.Context, partition P) (item E, err error) {
	items, _, err := t.QueryPage(ctx, partition, "", 1)
	if err != nil {
		return item, err
	}
	if len(items) == 0 {
		return item, errors.NotExist
	}
	return items[0], nil
}

func (t *DDBTable[E, P, S]) QueryLastOne(ctx context.Context, partition P) (item E, err error) {
	items, _, err := t.QueryPage(ctx, partition, "", 1, func(input *dynamodb.QueryInput) {
		input.ScanIndexForward = aws.Bool(false)
	})
	if err != nil {
		return item, err
	}
	if len(items) == 0 {
		return item, errors.NotExist
	}
	return items[0], nil
}

func (t *DDBTable[E, P, S]) createQueryInput(partition P, limit int32) (*dynamodb.QueryInput, error) {
	keyCond := expression.Key(t.primaryKey.PartitionKey).Equal(expression.Value(partition))
	cols := conv.MustSlice(t.columns, expression.Name)
	proj := expression.NamesList(cols[0], cols[1:]...)
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("expression.Build: %w", err)
	}

	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(t.tableName),
		IndexName:                 t.indexName,
		Limit:                     aws.Int32(limit),
	}
	return input, nil
}
