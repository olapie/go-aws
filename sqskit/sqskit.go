package sqskit

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.olapie.com/logs"
	"go.olapie.com/ola/activity"
	"go.olapie.com/ola/headers"
	"go.olapie.com/security/base62"
	"go.olapie.com/utils"
)

const (
	keyUserID      = "X-User-Id"
	DataTypeString = "String"
	DataTypeNumber = "Number"
)

func createMessageAttributesFromOutgoingContext(ctx context.Context) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)
	a := activity.FromOutgoingContext(ctx)
	if a == nil {
		logs.FromCtx(ctx).ErrorContext(ctx, "no activity in outgoing context")
		return attrs
	}

	if traceID := a.Get(headers.KeyTraceID); traceID != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String(DataTypeString),
			StringValue: aws.String(traceID),
		}
		attrs[headers.KeyTraceID] = attr
	}

	if login, _ := a.UserID().Int(); login != 0 {
		attr := types.MessageAttributeValue{
			DataType:    aws.String(DataTypeNumber),
			StringValue: aws.String(fmt.Sprint(login)),
		}
		attrs[keyUserID] = attr
	} else if login, _ := a.UserID().String(); login != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String(DataTypeString),
			StringValue: aws.String(login),
		}
		attrs[keyUserID] = attr
	}

	return attrs
}

func NewIncomingContextFromMessageAttributes(ctx context.Context, attrs map[string]events.SQSMessageAttribute) context.Context {
	a := activity.FromIncomingContext(ctx)
	if a == nil {
		a = activity.New("", http.Header{})
		ctx = activity.NewIncomingContext(ctx, a)
	}
	var traceID string
	if len(attrs) != 0 {
		if attr, ok := attrs[headers.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *attr.StringValue
		}

		if attr, ok := attrs[keyUserID]; ok && attr.StringValue != nil {
			if attr.DataType == DataTypeString {
				_ = activity.SetIncomingUserID(ctx, *attr.StringValue)
			} else {
				_ = activity.SetIncomingUserID(ctx, utils.MustToInt64(*attr.StringValue))
			}
		}
	}

	if traceID == "" {
		traceID = base62.NewUUIDString()
	}
	a.Set(headers.KeyTraceID, traceID)
	logger := logs.FromCtx(ctx).With(slog.String("trace_id", traceID))
	ctx = logs.NewCtx(ctx, logger)
	return ctx
}
