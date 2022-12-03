package apigateway

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

type Response = events.APIGatewayV2HTTPResponse

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
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.Plain
	resp.StatusCode = http.StatusOK
	resp.Body = http.StatusText(http.StatusOK)
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
	requestID := httpkit.GetHeader(request.Headers, httpkit.KeyRequestID)
	if requestID == "" {
		requestID = uuid.NewString()
	}

	appID := httpkit.GetHeader(request.Headers, httpkit.KeyApplicationID)
	if appID != "" {
		ctx = ctxutil.WithApplicationID(ctx, appID)
	}

	deviceID := httpkit.GetHeader(request.Headers, httpkit.KeyDeviceID)
	ctx = ctxutil.WithDeviceID(ctx, deviceID)

	logger := log.FromContext(ctx).With(log.String("request_id", requestID))
	ctx = log.BuildContext(ctx, logger)

	return ctx
}
