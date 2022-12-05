package lambda

import (
	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"code.olapie.com/log"
	"code.olapie.com/ola/ctxutil"
	"code.olapie.com/ola/httpkit"
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"net/http"
)

func Error(err error) *Response {
	if err == nil {
		return OK()
	}

	if er, ok := err.(*errors.Error); ok {
		return JSON(er.Code, er)
	}

	var er errors.Error
	er.Code = errors.GetCode(err)
	if er.Code == 0 {
		er.Code = http.StatusInternalServerError
	}
	er.Message = err.Error()
	return JSON(er.Code, er)
}

func OK() *Response {
	return Status(http.StatusOK)
}

func Status(s int) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.Plain
	resp.StatusCode = s
	resp.Body = http.StatusText(s)
	return resp
}

func NoContent() *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = http.StatusNoContent
	return resp
}

func JSON200(v any) *Response {
	return JSON(200, v)
}

func JSON200OrError(v any, err error) *Response {
	if err != nil {
		return Error(err)
	}
	return JSON(200, v)
}

func JSON201(v any) *Response {
	return JSON(201, v)
}

func JSON202(v any) *Response {
	return JSON(202, v)
}

func JSON(status int, v any) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.JSON
	resp.Body = conv.MustJSONString(v)
	return resp
}

func BuildContext(ctx context.Context, request *events.APIGatewayV2HTTPRequest) context.Context {
	appID := httpkit.GetHeader(request.Headers, httpkit.KeyApplicationID)
	if appID == "" {
		appID = request.QueryStringParameters["application_id"]
	}
	if appID != "" {
		ctx = ctxutil.WithApplicationID(ctx, appID)
	}

	clientID := httpkit.GetHeader(request.Headers, httpkit.KeyClientID)
	if clientID == "" {
		clientID = request.QueryStringParameters["client_id"]
	}
	ctx = ctxutil.WithClientID(ctx, clientID)

	traceID := httpkit.GetHeader(request.Headers, httpkit.KeyTraceID)
	if traceID == "" {
		traceID = request.QueryStringParameters["trace_id"]
		if traceID == "" {
			traceID = uuid.NewString()
		}
	}
	ctx = ctxutil.WithTraceID(ctx, traceID)

	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = log.BuildContext(ctx, logger)

	return ctx
}
