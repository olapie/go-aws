package aws

import "net/http"

type ErrorString string

func (s ErrorString) Error() string {
	return string(s)
}

func (s ErrorString) Code() int {
	switch s {
	case ErrInvalidToken:
		return http.StatusBadRequest
	case ErrItemNotFound, ErrKeyNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

func (s ErrorString) Status() int {
	return s.Code()
}

const (
	ErrInvalidToken ErrorString = "invalid token"
	ErrItemNotFound ErrorString = "item not found"
	ErrKeyNotFound  ErrorString = "key not found"
)
