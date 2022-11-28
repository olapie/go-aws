package awskit

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type SMS struct {
}

func NewSMS(cfg aws.Config) *SMS {
	return &SMS{}
}

func (s *SMS) Send(ctx context.Context, recipient []string, text string) (string, error) {
	// TODO:
	return "", nil
}
