package ddb

import (
	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/exp/constraints"
	"reflect"
)

// NoKey means table doesn't have sort key
type NoKey *any

type PartitionKeyConstraint interface {
	~string | constraints.Signed | constraints.Unsigned
}

type SortKeyConstraint interface {
	PartitionKeyConstraint | NoKey
}

type PrimaryKeyDefinition[P PartitionKeyConstraint, S SortKeyConstraint] struct {
	PartitionKeyName string
	SortKeyName      string

	prototype map[string]reflect.Type
}

func (d *PrimaryKeyDefinition[P, S]) NewKey(p P, s S) *PrimaryKey[P, S] {
	return &PrimaryKey[P, S]{
		PartitionKey: p,
		SortKey:      s,
		definition:   d,
	}
}

func (d *PrimaryKeyDefinition[P, S]) HasSortKey() bool {
	if d.SortKeyName == "" {
		return false
	}
	var s S
	if _, ok := any(s).(NoKey); ok {
		return false
	}
	return true
}

func (d *PrimaryKeyDefinition[P, S]) Prototype() map[string]reflect.Type {
	if d.prototype != nil {
		return d.prototype
	}
	var p P
	var s S
	key := d.NewKey(p, s).AttributeValue()
	nameToType := make(map[string]reflect.Type, len(key))
	for name, attr := range key {
		nameToType[name] = reflect.TypeOf(attr)
	}
	return nameToType
}

func (d *PrimaryKeyDefinition[P, S]) DecodeStringToValue(s string) (map[string]types.AttributeValue, error) {
	jsonBytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("base64.DecodeString: %w", err)
	}
	var nameToValue map[string]any
	err = json.Unmarshal(jsonBytes, &nameToValue)
	if err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %w", err)
	}

	key := make(map[string]types.AttributeValue, len(d.Prototype()))
	for name, val := range nameToValue {
		typ, ok := d.prototype[name]
		if !ok {
			return nil, errors.BadRequest("invalid token")
		}
		attr := reflect.New(typ)
		err = json.Unmarshal(conv.MustJSONBytes(val), attr.Interface())
		if err != nil {
			return nil, errors.BadRequest("invalid token")
		}
		key[name] = attr.Elem().Interface().(types.AttributeValue)
	}

	return key, nil
}

func (d *PrimaryKeyDefinition[P, S]) EncodeValueToString(v map[string]types.AttributeValue) string {
	return base64.StdEncoding.EncodeToString(conv.MustJSONBytes(v))
}

type PrimaryKey[P PartitionKeyConstraint, S SortKeyConstraint] struct {
	PartitionKey P
	SortKey      S

	definition *PrimaryKeyDefinition[P, S]
}

func (pk *PrimaryKey[P, S]) AttributeValue() map[string]types.AttributeValue {
	attrs := make(map[string]types.AttributeValue)
	if str, ok := any(pk.PartitionKey).(string); ok {
		attrs[pk.definition.PartitionKeyName] = &types.AttributeValueMemberS{
			Value: str,
		}
	} else {
		attrs[pk.definition.PartitionKeyName] = &types.AttributeValueMemberN{
			Value: fmt.Sprint(pk.PartitionKey),
		}
	}

	if _, ok := any(pk.SortKey).(NoKey); ok {
		return attrs
	}

	if !pk.definition.HasSortKey() {
		panic("sort key is not defined")
	}

	if str, ok := any(pk.SortKey).(string); ok {
		attrs[pk.definition.SortKeyName] = &types.AttributeValueMemberS{
			Value: str,
		}
	} else {
		attrs[pk.definition.SortKeyName] = &types.AttributeValueMemberN{
			Value: fmt.Sprint(pk.SortKey),
		}
	}

	return attrs
}
