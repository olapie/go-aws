package awskit

import (
	"code.olapie.com/sugar/xcontact/v2"
	"context"
	"errors"
	"fmt"

	"code.olapie.com/sugar/v2/xruntime"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type SNS struct {
	c *sns.Client
}

func NewSNS(cfg aws.Config) *SNS {
	return &SNS{
		c: sns.NewFromConfig(cfg),
	}
}

func (s *SNS) SendMobileMessage(ctx context.Context, recipient *xcontact.PhoneNumber, message string, optFns ...func(*sns.Options)) (string, error) {
	input := &sns.PublishInput{
		Message:     xruntime.Addr(message),
		PhoneNumber: xruntime.Addr(recipient.InternationalFormat()),
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
