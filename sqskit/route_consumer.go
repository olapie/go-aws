package sqskit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"go.olapie.com/router"
	"go.olapie.com/x/xerror"
	"go.olapie.com/x/xlog"
)

type RoutableMessage struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Body   []byte `json:"body"`
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

	logger := xlog.FromContext(ctx)
	logger.Info("START",
		slog.String("method", message.Method),
		slog.String("path", message.Path))

	endpoint, _ := c.Match(message.Method, message.Path)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		return handler.Handler()(ctx, message.Body)
	}
	return xerror.NotFound("endpoint not found")
}
