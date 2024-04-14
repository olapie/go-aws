package sqskit

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.olapie.com/x/xbase62"
	"go.olapie.com/x/xcontext"
	"go.olapie.com/x/xconv"
	"go.olapie.com/x/xhttpheader"
	"go.olapie.com/x/xlog"
)

const (
	keyUserID      = "X-User-Id"
	DataTypeString = "String"
	DataTypeNumber = "Number"
)

func createMessageAttributesFromOutgoingContext(ctx context.Context) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)
	a := xcontext.GetOutgoingActivity(ctx)
	if a == nil {
		xlog.FromContext(ctx).ErrorContext(ctx, "no activity in outgoing context")
		return attrs
	}

	if traceID := a.Get(xhttpheader.KeyTraceID); traceID != "" {
		attr := types.MessageAttributeValue{
			DataType:    aws.String(DataTypeString),
			StringValue: aws.String(traceID),
		}
		attrs[xhttpheader.KeyTraceID] = attr
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
	a := xcontext.GetIncomingActivity(ctx)
	if a == nil {
		a = xcontext.NewActivity("", http.Header{})
		ctx = xcontext.WithIncomingActivity(ctx, a)
	}
	var traceID string
	if len(attrs) != 0 {
		if attr, ok := attrs[xhttpheader.KeyTraceID]; ok && attr.StringValue != nil {
			traceID = *attr.StringValue
		}

		if attr, ok := attrs[keyUserID]; ok && attr.StringValue != nil {
			if attr.DataType == DataTypeString {
				_ = xcontext.SetIncomingUserID(ctx, *attr.StringValue)
			} else {
				_ = xcontext.SetIncomingUserID(ctx, xconv.MustToInt64(*attr.StringValue))
			}
		}
	}

	if traceID == "" {
		traceID = xbase62.NewUUIDString()
	}
	a.Set(xhttpheader.KeyTraceID, traceID)
	logger := xlog.FromContext(ctx).With(slog.String("trace_id", traceID))
	ctx = xlog.NewCtx(ctx, logger)
	return ctx
}
