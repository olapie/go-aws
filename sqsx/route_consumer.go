package sqsx

import (
	"context"
	"encoding/json"
	"fmt"

	"code.olapie.com/log"
	"code.olapie.com/router"
	"code.olapie.com/sugar/contexts"
	"code.olapie.com/sugar/errorx"
)

type RoutableMessage struct {
	Method  string `json:"method"`
	Path    string `json:"path"`
	TraceID string `json:"trace_id"`
	Body    []byte `json:"body"`
}

type RoutableMessageHandlerFunc = router.HandlerFunc[[]byte, error]

type Router = router.Router[RoutableMessageHandlerFunc]

type RoutableMessageConsumer struct {
	*Router
	*MessageConsumer
}

func NewRoutableMessageConsumer(queueName string, api ReceiveMessageAPI, optFns ...func(options *RawConsumerOptions)) *RoutableMessageConsumer {
	c := &RoutableMessageConsumer{
		Router: router.New[RoutableMessageHandlerFunc](),
	}
	c.MessageConsumer = NewMessageConsumer(queueName, api, c, optFns...)
	return c
}

func (c *RoutableMessageConsumer) HandleMessage(ctx context.Context, rawMessage string) error {
	var message RoutableMessage
	err := json.Unmarshal([]byte(rawMessage), &message)
	if err != nil {
		return fmt.Errorf("unmarshal to routable message: %w", err)
	}

	ctx = contexts.WithTraceID(ctx, message.TraceID)
	logger := log.FromContext(ctx)
	logger.Info("received", log.String("method", message.Method), log.String("path", message.Path))

	endpoint, _ := c.Match(message.Method, message.Path)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		return handler.Handler()(ctx, message.Body)
	}
	return errorx.NotFound("endpoint not found")
}
