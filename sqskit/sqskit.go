package sqskit

import (
	"code.olapie.com/log"
	"code.olapie.com/sugar/contexts"
	"code.olapie.com/sugar/httpx"
	"code.olapie.com/sugar/must"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

func BuildMessageAttributesFromContext(ctx context.Context) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)
	if traceID := contexts.GetTraceID(ctx); traceID != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(traceID),
		}
		attrs[httpx.KeyTraceID] = attr
	}

	if login := contexts.GetLogin[int64](ctx); login != 0 {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprint(login)),
		}
		attrs[httpx.KeyUserID] = attr
	} else if login := contexts.GetLogin[string](ctx); login != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(login),
		}
		attrs[httpx.KeyUserID] = attr
	}

	return attrs
}

func BuildContextFromMessageAttributes(ctx context.Context, attrs map[string]events.SQSMessageAttribute) context.Context {
	var traceID string
	if len(attrs) != 0 {
		if attr, ok := attrs[httpx.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *attr.StringValue
			ctx = contexts.WithTraceID(ctx, *attr.StringValue)
		}

		if attr, ok := attrs[httpx.KeyUserID]; ok && attr.StringValue != nil {
			if attr.DataType == "String" {
				ctx = contexts.WithLogin(ctx, *attr.StringValue)
			} else {
				ctx = contexts.WithLogin(ctx, must.ToInt64(*attr.StringValue))
			}
		}
	}

	if traceID == "" {
		traceID = uuid.NewString()
	}

	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = contexts.WithTraceID(ctx, traceID)
	ctx = contexts.WithLogger(ctx, logger)
	return ctx
}
