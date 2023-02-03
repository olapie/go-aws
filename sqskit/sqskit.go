package sqskit

import (
	"context"
	"fmt"

	"code.olapie.com/log"
	"code.olapie.com/sugar/v2/base62"
	"code.olapie.com/sugar/v2/must"
	"code.olapie.com/sugar/v2/xcontext"
	"code.olapie.com/sugar/v2/xhttp"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func BuildMessageAttributesFromContext(ctx context.Context) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)
	if traceID := xcontext.GetTraceID(ctx); traceID != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(traceID),
		}
		attrs[xhttp.KeyTraceID] = attr
	}

	if login := xcontext.GetLogin[int64](ctx); login != 0 {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprint(login)),
		}
		attrs[xhttp.KeyUserID] = attr
	} else if login := xcontext.GetLogin[string](ctx); login != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(login),
		}
		attrs[xhttp.KeyUserID] = attr
	}

	return attrs
}

func BuildContextFromMessageAttributes(ctx context.Context, attrs map[string]events.SQSMessageAttribute) context.Context {
	var traceID string
	if len(attrs) != 0 {
		if attr, ok := attrs[xhttp.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *attr.StringValue
			ctx = xcontext.WithTraceID(ctx, *attr.StringValue)
		}

		if attr, ok := attrs[xhttp.KeyUserID]; ok && attr.StringValue != nil {
			if attr.DataType == "String" {
				ctx = xcontext.WithLogin(ctx, *attr.StringValue)
			} else {
				ctx = xcontext.WithLogin(ctx, must.ToInt64(*attr.StringValue))
			}
		}
	}

	if traceID == "" {
		traceID = base62.NewUUIDString()
	}

	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = xcontext.WithTraceID(ctx, traceID)
	ctx = log.BuildContext(ctx, logger)
	return ctx
}
