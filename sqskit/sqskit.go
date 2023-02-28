package sqskit

import (
	"context"
	"fmt"
	"go.olapie.com/rpcx/httpx"
	"go.olapie.com/utils"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.olapie.com/log"
	"go.olapie.com/security/base62"
)

func BuildMessageAttributesFromContext(ctx context.Context) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)
	if traceID := utils.GetTraceID(ctx); traceID != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(traceID),
		}
		attrs[httpx.KeyTraceID] = attr
	}

	if login := utils.GetLogin[int64](ctx); login != 0 {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprint(login)),
		}
		attrs["X-User-Id"] = attr
	} else if login := utils.GetLogin[string](ctx); login != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(login),
		}
		attrs["X-User-Id"] = attr
	}

	return attrs
}

func BuildContextFromMessageAttributes(ctx context.Context, attrs map[string]events.SQSMessageAttribute) context.Context {
	var traceID string
	if len(attrs) != 0 {
		if attr, ok := attrs[httpx.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *attr.StringValue
		}

		if attr, ok := attrs["X-User-Id"]; ok && attr.StringValue != nil {
			if attr.DataType == "String" {
				ctx = utils.WithLogin(ctx, *attr.StringValue)
			} else {
				ctx = utils.WithLogin(ctx, utils.MustToInt64(*attr.StringValue))
			}
		}
	}

	if traceID == "" {
		traceID = base62.NewUUIDString()
	}

	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = utils.NewRequestContextBuilder(ctx).WithTraceID(traceID).Build()
	ctx = log.BuildContext(ctx, logger)
	return ctx
}
