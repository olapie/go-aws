package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"go.olapie.com/x/xconv"
)

type SNS struct {
	c *sns.Client
}

func NewSNS(cfg aws.Config) *SNS {
	return &SNS{
		c: sns.NewFromConfig(cfg),
	}
}

func (s *SNS) SendMobileMessage(ctx context.Context, recipient string, message string, optFns ...func(*sns.Options)) (string, error) {
	input := &sns.PublishInput{
		Message:     xconv.Pointer(message),
		PhoneNumber: xconv.Pointer(recipient),
	}
	output, err := s.c.Publish(ctx, input, optFns...)
	if err != nil {
		return "", fmt.Errorf("publish: %w", err)
	}

	if output.MessageId == nil {
		return "", errors.New("output.MessageId is nil")
	}
	return *output.MessageId, nil
}
