package awskit

import (
	"context"
	"errors"
	"fmt"

	"code.olapie.com/sugar/contacts"
	"code.olapie.com/sugar/v2/rt"
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

func (s *SNS) SendMobileMessage(ctx context.Context, recipient *contacts.PhoneNumber, message string, optFns ...func(*sns.Options)) (string, error) {
	input := &sns.PublishInput{
		Message:     rt.Addr(message),
		PhoneNumber: rt.Addr(recipient.InternationalFormat()),
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
