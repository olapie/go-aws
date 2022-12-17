package sqskit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"code.olapie.com/log"
	"code.olapie.com/sugar/ctxutil"
	"code.olapie.com/sugar/httpx"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

const MaxVisibilityTimeout = 60 * 60 // one hour

// ReceiveMessageAPI defines the interface for get queue url, receive and delete messages.
// sqs.Client implements this interface
type ReceiveMessageAPI interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options),
	) (*sqs.GetQueueUrlOutput, error)

	ReceiveMessage(ctx context.Context,
		params *sqs.ReceiveMessageInput,
		optFns ...func(*sqs.Options),
	) (*sqs.ReceiveMessageOutput, error)

	DeleteMessage(ctx context.Context,
		params *sqs.DeleteMessageInput,
		optFns ...func(*sqs.Options),
	) (*sqs.DeleteMessageOutput, error)
}

type RawConsumerOptions struct {
	VisibilityTimeout   int32
	MaxNumberOfMessages int32
}

type MessageHandler interface {
	HandleMessage(ctx context.Context, message string) error
}

type MessageHandlerFunc func(ctx context.Context, message string) error

func (h MessageHandlerFunc) HandleMessage(ctx context.Context, message string) error {
	return h(ctx, message)
}

type MessageConsumer struct {
	queueName string
	queueURL  *string

	api     ReceiveMessageAPI
	handler MessageHandler

	options *RawConsumerOptions
}

func NewMessageConsumer(queueName string, api ReceiveMessageAPI, handler MessageHandler, optFns ...func(options *RawConsumerOptions)) *MessageConsumer {
	c := &MessageConsumer{
		api:       api,
		handler:   handler,
		queueName: queueName,
		options: &RawConsumerOptions{
			MaxNumberOfMessages: 1,
		},
	}

	for _, fn := range optFns {
		fn(c.options)
	}

	if c.options.VisibilityTimeout < 0 {
		panic(fmt.Sprintf("invalid options.visibilityTimeout %d", c.options.VisibilityTimeout))
	}

	if c.options.VisibilityTimeout > MaxVisibilityTimeout {
		c.options.VisibilityTimeout = MaxVisibilityTimeout
	}

	if c.options.MaxNumberOfMessages <= 0 {
		panic(fmt.Sprintf("invalid options.visibilityTimeout %d", c.options.MaxNumberOfMessages))
	}

	return c
}

// Start starts consumer message loop
// If it's a service, ctx must never time out
func (c *MessageConsumer) Start(ctx context.Context) {
	logger := log.FromContext(ctx).With(log.String("queue_name", c.queueName))
	ctx = log.BuildContext(ctx, logger)
	c.getQueueURL(ctx, 10)
	if c.queueURL == nil {
		logger.Info("Stopping consumer due to no queue url")
		return
	}

	input := &sqs.ReceiveMessageInput{
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
		QueueUrl:            c.queueURL,
		MaxNumberOfMessages: c.options.MaxNumberOfMessages,
		VisibilityTimeout:   c.options.VisibilityTimeout,
	}

	backoff := 100 * time.Millisecond
	for {
		err := c.receiveMessage(ctx, input)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			logger.Error("receive sqs message", log.Error(err))
			return
		}

		if err != nil {
			logger.Error("receive sqs message", log.Error(err))
			backoff += 100 * time.Millisecond
			time.Sleep(backoff)
		}
	}
}

func (c *MessageConsumer) getQueueURL(ctx context.Context, retries int) {
	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(c.queueName),
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for i := 0; i < retries; i++ {
		output, err := c.api.GetQueueUrl(ctx, input)
		if err == nil {
			c.queueURL = output.QueueUrl
			break
		}
		log.FromContext(ctx).Error("get queue url", log.Error(err))
	}
}

func (c *MessageConsumer) receiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput) error {
	output, err := c.api.ReceiveMessage(ctx, input)
	logger := log.FromContext(ctx)
	if err != nil {
		logger.Error("ReceiveMessage", log.Error(err))
		return err
	}

	logger.Sugar().Errorf("Received %d messages\n", len(output.Messages))

	for _, msg := range output.Messages {
		var msgID string
		if msg.MessageId != nil {
			msgID = *msg.MessageId
		}

		var traceID string
		if attr, ok := msg.MessageAttributes[httpx.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *(attr.StringValue)
		} else {
			traceID = uuid.NewString()
		}
		ctx = ctxutil.WithTraceID(ctx, traceID)
		msgLogger := logger.With(log.String("trace_id", traceID))
		msgLogger.Info("START", log.String("message_id", msgID))

		if msg.Body == nil || *msg.Body == "" {
			msgLogger.Warn("empty message")
			continue
		}

		if err = c.handler.HandleMessage(ctx, *msg.Body); err != nil {
			msgLogger.Error("handler.HandleMessage", log.Error(err))
			continue
		}

		msgLogger.Info("END")

		_, err = c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      c.queueURL,
			ReceiptHandle: msg.ReceiptHandle,
		})

		if err != nil {
			msgLogger.Warn("api.DeleteMessage", log.Error(err))
		}
	}
	return nil
}
