package sqskit

import (
	"code.olapie.com/sugar/v2/conv"
	"context"
	"fmt"

	"code.olapie.com/log"
	"code.olapie.com/sugar/v2/base62"
	"code.olapie.com/sugar/v2/ctxutil"
	"code.olapie.com/sugar/v2/httpkit"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func BuildMessageAttributesFromContext(ctx context.Context) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)
	if traceID := ctxutil.GetTraceID(ctx); traceID != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(traceID),
		}
		attrs[httpkit.KeyTraceID] = attr
	}

	if login := ctxutil.GetLogin[int64](ctx); login != 0 {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprint(login)),
		}
		attrs[httpkit.KeyUserID] = attr
	} else if login := ctxutil.GetLogin[string](ctx); login != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(login),
		}
		attrs[httpkit.KeyUserID] = attr
	}

	return attrs
}

func BuildContextFromMessageAttributes(ctx context.Context, attrs map[string]events.SQSMessageAttribute) context.Context {
	var traceID string
	if len(attrs) != 0 {
		if attr, ok := attrs[httpkit.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *attr.StringValue
		}

		if attr, ok := attrs[httpkit.KeyUserID]; ok && attr.StringValue != nil {
			if attr.DataType == "String" {
				ctx = ctxutil.WithLogin(ctx, *attr.StringValue)
			} else {
				ctx = ctxutil.WithLogin(ctx, conv.MustToInt64(*attr.StringValue))
			}
		}
	}

	if traceID == "" {
		traceID = base62.NewUUIDString()
	}

	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = ctxutil.Request(ctx).WithTraceID(traceID).Build()
	ctx = log.BuildContext(ctx, logger)
	return ctx
}
