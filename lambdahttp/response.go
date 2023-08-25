package lambdahttp

import (
	"context"
	"encoding/json"
	"go.olapie.com/ola/mimetypes"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"go.olapie.com/logs"
	"go.olapie.com/ola/activity"
	"go.olapie.com/ola/errorutil"
	"go.olapie.com/ola/headers"
)

func Error(err error) *Response {
	if err == nil {
		return OK()
	}

	return JSON(errorutil.GetCode(err), err.Error())
}

func OK() *Response {
	return Status(http.StatusOK)
}

func Status(s int) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	headers.SetContentType(resp.Headers, mimetypes.Plain)
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
	headers.SetContentType(resp.Headers, mimetypes.JSON)
	body, _ := json.Marshal(v)
	resp.Body = string(body)
	return resp
}

func CSS200(cssText string) *Response {
	return CSS(http.StatusOK, cssText)
}

func CSS(status int, cssText string) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	headers.SetContentType(resp.Headers, mimetypes.CSS)
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
	headers.SetContentType(resp.Headers, mimetypes.HtmlUTF8)
	resp.Body = htmlText
	return resp
}

func Text200OrError(text string, err error) *Response {
	if err != nil {
		return Error(err)
	}
	return Text(http.StatusOK, text)
}

func Text(status int, text string) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	headers.SetContentType(resp.Headers, mimetypes.Plain)
	resp.Body = text
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
	resp.Headers[headers.KeyLocation] = location
	return resp
}

func buildContext(ctx context.Context, request *Request) context.Context {
	a := activity.New("", request.Headers)
	ctx = activity.NewIncomingContext(ctx, a)
	traceID := a.GetTraceID()
	if traceID == "" {
		traceID = uuid.NewString()
		a.SetTraceID(traceID)
	}
	logger := logs.FromContext(ctx).With(slog.String("trace_id", traceID))
	ctx = logs.NewContext(ctx, logger)
	return ctx
}
