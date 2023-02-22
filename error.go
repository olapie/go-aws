package awskit

import "code.olapie.com/sugar/v2/xerror"

type ErrorString string

func (s ErrorString) Error() string {
	return string(s)
}

const (
	ErrInvalidToken xerror.String = "invalid token"
	ErrItemNotFound xerror.String = "item not found"
	ErrKeyNotFound  xerror.String = "key not found"
)
