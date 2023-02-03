package ddb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"code.olapie.com/sugar/v2/xerror"
	"code.olapie.com/sugar/v2/xjson"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/exp/constraints"
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
	partitionKeyName string
	sortKeyName      string

	prototype     map[string]reflect.Type
	attrNotExists *string
	attrExists    *string
}

func NewPrimaryKeyDefinition[P PartitionKeyConstraint, S SortKeyConstraint](partitionKeyName string, sortKeyName string) *PrimaryKeyDefinition[P, S] {
	d := &PrimaryKeyDefinition[P, S]{
		partitionKeyName: partitionKeyName,
		sortKeyName:      sortKeyName,
	}

	var p P
	var s S
	key := d.NewKey(p, s).AttributeValue()
	d.prototype = make(map[string]reflect.Type, len(key))
	for name, attr := range key {
		d.prototype[name] = reflect.TypeOf(attr)
	}

	attrNotExists := "attribute_not_exists(" + partitionKeyName + ")"
	if sortKeyName != "" {
		attrNotExists += " AND attribute_not_exists(" + sortKeyName + ")"
	}
	d.attrNotExists = aws.String(attrNotExists)
	d.attrExists = aws.String(strings.Replace(attrNotExists, "attribute_not_exists", "attribute_exists", -1))
	return d
}

func (d *PrimaryKeyDefinition[P, S]) NewKey(p P, s S) *PrimaryKey[P, S] {
	return &PrimaryKey[P, S]{
		PartitionKey: p,
		SortKey:      s,
		definition:   d,
	}
}

func (d *PrimaryKeyDefinition[P, S]) NewKeys(partitionKeys []P, sortKeys []S) []*PrimaryKey[P, S] {
	pks := make([]*PrimaryKey[P, S], len(partitionKeys))
	for i, p := range partitionKeys {
		pks[i] = &PrimaryKey[P, S]{
			PartitionKey: p,
			definition:   d,
		}
		if sortKeys != nil {
			pks[i].SortKey = sortKeys[i]
		}
	}
	return pks
}

func (d *PrimaryKeyDefinition[P, S]) HasSortKey() bool {
	if d.sortKeyName == "" {
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
		typ, ok := d.Prototype()[name]
		if !ok {
			return nil, xerror.BadRequest("invalid token")
		}
		attr := reflect.New(typ)
		err = json.Unmarshal(xjson.ToBytes(val), attr.Interface())
		if err != nil {
			return nil, xerror.BadRequest("invalid token")
		}
		key[name] = attr.Elem().Interface().(types.AttributeValue)
	}

	return key, nil
}

func (d *PrimaryKeyDefinition[P, S]) EncodeValueToString(v map[string]types.AttributeValue) string {
	return base64.StdEncoding.EncodeToString(xjson.ToBytes(v))
}

type PrimaryKey[P PartitionKeyConstraint, S SortKeyConstraint] struct {
	PartitionKey P
	SortKey      S

	definition *PrimaryKeyDefinition[P, S]
}

func (pk *PrimaryKey[P, S]) AttributeValue() map[string]types.AttributeValue {
	attrs := make(map[string]types.AttributeValue)
	if str, ok := any(pk.PartitionKey).(string); ok {
		attrs[pk.definition.partitionKeyName] = &types.AttributeValueMemberS{
			Value: str,
		}
	} else {
		attrs[pk.definition.partitionKeyName] = &types.AttributeValueMemberN{
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
		attrs[pk.definition.sortKeyName] = &types.AttributeValueMemberS{
			Value: str,
		}
	} else {
		attrs[pk.definition.sortKeyName] = &types.AttributeValueMemberN{
			Value: fmt.Sprint(pk.SortKey),
		}
	}

	return attrs
}
