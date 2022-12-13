package sqsx

import (
	"context"
	"encoding/json"
	"fmt"

	"code.olapie.com/sugar/contexts"
)

type RoutableMessageProducer struct {
	producer *MessageProducer
}

func NewRoutableMessageProducer(queueName string, api SendMessageAPI) *RoutableMessageProducer {
	c := &RoutableMessageProducer{
		producer: NewMessageProducer(queueName, api),
	}
	return c
}

func (c *RoutableMessageProducer) SendMessage(ctx context.Context, method, path string, body []byte) (string, error) {
	return c.SendDelayMessage(ctx, method, path, body, 0)
}

func (c *RoutableMessageProducer) SendDelayMessage(ctx context.Context, method, path string, body []byte, delaySeconds int32) (string, error) {
	message := &RoutableMessage{
		Method:  method,
		Path:    path,
		TraceID: contexts.GetTraceID(ctx),
		Body:    body,
	}

	data, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("json.Marshal: %w", err)
	}

	return c.producer.SendDelayMessage(ctx, string(data), delaySeconds)
}
