package sqskit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.olapie.com/x/xcontext"
	"go.olapie.com/x/xhttpheader"
	"go.olapie.com/x/xlog"
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
	logger := xlog.FromContext(ctx).With(slog.String("queue_name", c.queueName))
	ctx = xlog.NewCtx(ctx, logger)
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
			logger.Error("receive sqs message", xlog.Err(err))
			return
		}

		if err != nil {
			logger.Error("receive sqs message", xlog.Err(err))
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
		xlog.FromContext(ctx).Error("get queue url", xlog.Err(err))
	}
}

func (c *MessageConsumer) receiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput) error {
	output, err := c.api.ReceiveMessage(ctx, input)
	logger := xlog.FromContext(ctx)
	if err != nil {
		logger.Error("ReceiveMessage", xlog.Err(err))
		return err
	}

	logger.Info(fmt.Sprintf("Received %d messages\n", len(output.Messages)))

	for _, msg := range output.Messages {
		var msgID string
		if msg.MessageId != nil {
			msgID = *msg.MessageId
		}

		var traceID string
		if attr, ok := msg.MessageAttributes[xhttpheader.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *(attr.StringValue)
		} else {
			traceID = uuid.NewString()
		}
		a := xcontext.NewActivity("", http.Header{})
		ctx = xcontext.WithIncomingActivity(ctx, a)
		a.Set(xhttpheader.KeyTraceID, traceID)
		msgLogger := logger.With(slog.String("trace_id", traceID))
		msgLogger.Info("START", slog.String("message_id", msgID))

		if msg.Body == nil || *msg.Body == "" {
			msgLogger.Warn("empty message")
			continue
		}

		if err = c.handler.HandleMessage(ctx, *msg.Body); err != nil {
			msgLogger.Error("handler.HandleMessage", xlog.Err(err))
			continue
		}

		msgLogger.Info("END")

		_, err = c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      c.queueURL,
			ReceiptHandle: msg.ReceiptHandle,
		})

		if err != nil {
			msgLogger.Warn("delete message", xlog.Err(err))
		}
	}
	return nil
}
