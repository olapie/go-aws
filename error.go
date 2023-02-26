package aws

type ErrorString string

func (s ErrorString) Error() string {
	return string(s)
}

const (
	ErrInvalidToken ErrorString = "invalid token"
	ErrItemNotFound ErrorString = "item not found"
	ErrKeyNotFound  ErrorString = "key not found"
)
