package awskit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	"code.olapie.com/conv"
	"code.olapie.com/errors"
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

	prototype map[string]reflect.Type
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

	if !pk.HasSortKey() {
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

func (pk *DDBPrimaryKey[P, S]) HasSortKey() bool {
	if pk.SortKey == "" {
		return false
	}
	var s S
	if _, ok := any(s).(DDBNoSortKey); ok {
		return false
	}
	return true
}

func (pk *DDBPrimaryKey[P, S]) Prototype() map[string]reflect.Type {
	if pk.prototype != nil {
		return pk.prototype
	}
	var p P
	var s S
	key := pk.AttributeValue(p, s)
	nameToType := make(map[string]reflect.Type, len(key))
	for name, attr := range key {
		nameToType[name] = reflect.TypeOf(attr)
	}
	return nameToType
}

func (pk *DDBPrimaryKey[P, S]) DecodeAttributeValue(s string) (map[string]types.AttributeValue, error) {
	jsonBytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("base64.DecodeString: %w", err)
	}
	var nextKeyMap map[string]any
	err = json.Unmarshal(jsonBytes, &nextKeyMap)
	if err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %w", err)
	}

	key := make(map[string]types.AttributeValue, len(pk.Prototype()))
	for name, val := range nextKeyMap {
		typ, ok := pk.prototype[name]
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

func (pk *DDBPrimaryKey[P, S]) EncodeAttributeValue(v map[string]types.AttributeValue) string {
	return base64.StdEncoding.EncodeToString(conv.MustJSONBytes(v))
}
