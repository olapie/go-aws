package lambdahttp

import (
	"context"
	"net/http"

	"code.olapie.com/log"
	"code.olapie.com/sugar/ctxutil"
	"code.olapie.com/sugar/errorx"
	"code.olapie.com/sugar/httpx"
	"code.olapie.com/sugar/jsonx"
	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
)

func Error(err error) *Response {
	if err == nil {
		return OK()
	}

	if er, ok := err.(*errorx.Error); ok {
		return JSON(er.Code, er)
	}

	var er errorx.Error
	er.Code = errorx.GetCode(err)
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
	resp.Headers[httpx.KeyContentType] = httpx.Plain
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
	resp.Headers[httpx.KeyContentType] = httpx.JSON
	resp.Body = jsonx.ToString(v)
	return resp
}

func CSS200(cssText string) *Response {
	return CSS(http.StatusOK, cssText)
}

func CSS(status int, cssText string) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	resp.Headers[httpx.KeyContentType] = httpx.CSS
	resp.Body = cssText
	return resp
}

func HTML200(htmlText string) *Response {
	return HTML(http.StatusOK, htmlText)
}

func HTML200OrError(htmlText string, err error) *Response {
	if err != nil {
		return Error(err)
	}
	return HTML(http.StatusOK, htmlText)
}

func HTML(status int, htmlText string) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	resp.Headers[httpx.KeyContentType] = httpx.HtmlUTF8
	resp.Body = htmlText
	return resp
}

func Redirect(permanent bool, location string) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	if permanent {
		resp.StatusCode = http.StatusMovedPermanently
	} else {
		resp.StatusCode = http.StatusFound
	}
	resp.Headers = make(map[string]string)
	resp.Headers[httpx.KeyLocation] = location
	return resp
}

func BuildContext(ctx context.Context, request *Request) context.Context {
	appID := httpx.GetHeader(request.Headers, httpx.KeyAppID)
	clientID := httpx.GetHeader(request.Headers, httpx.KeyClientID)
	traceID := httpx.GetHeader(request.Headers, httpx.KeyTraceID)
	if traceID == "" {
		traceID = uuid.NewString()
	}
	ctx = ctxutil.WithAppID(ctx, appID)
	ctx = ctxutil.WithClientID(ctx, clientID)
	ctx = ctxutil.WithTraceID(ctx, traceID)
	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = log.BuildContext(ctx, logger)
	return ctx
}
