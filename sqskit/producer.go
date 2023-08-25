package sqskit

import (
	"context"
	"errors"
	"time"

	"go.olapie.com/logs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SendMessageAPI defines the interface for the GetQueueUrl and SendMessage functions.
// We use this interface to test the functions using a mocked service.
type SendMessageAPI interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)

	SendMessage(ctx context.Context,
		params *sqs.SendMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type MessageProducer struct {
	queueName string
	queueURL  *string

	api SendMessageAPI
}

func NewMessageProducer(queueName string, api SendMessageAPI) *MessageProducer {
	c := &MessageProducer{
		api:       api,
		queueName: queueName,
	}
	go c.getQueueURL(context.TODO(), 10)
	return c
}

func (c *MessageProducer) getQueueURL(ctx context.Context, retries int) {
	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(c.queueName),
	}

	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		output, err := c.api.GetQueueUrl(ctx, input)
		cancel()
		if err == nil {
			c.queueURL = output.QueueUrl
			break
		}
		logs.FromContext(ctx).Error("get queue url", logs.Err(err))
	}
}

func (c *MessageProducer) SendMessage(ctx context.Context, message string) (string, error) {
	return c.SendDelayMessage(ctx, message, 0)
}

func (c *MessageProducer) SendDelayMessage(ctx context.Context, message string, delaySeconds int32) (string, error) {
	input := &sqs.SendMessageInput{
		MessageBody:       aws.String(message),
		QueueUrl:          c.queueURL,
		DelaySeconds:      delaySeconds,
		MessageAttributes: createMessageAttributesFromOutgoingContext(ctx),
	}

	output, err := c.api.SendMessage(ctx, input)
	if err != nil {
		return "", err
	}
	if output.MessageId == nil {
		return "", errors.New("output.MessageId is nil")
	}
	return *output.MessageId, nil
}
