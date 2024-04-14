package lambdahttp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"go.olapie.com/x/xcontext"
	"go.olapie.com/x/xerror"
	"go.olapie.com/x/xhttpheader"
	"go.olapie.com/x/xlog"
	"go.olapie.com/x/xmime"
)

func Error(err error) *Response {
	if err == nil {
		return OK()
	}

	return JSON(xerror.GetCode(err), err.Error())
}

func OK() *Response {
	return Status(http.StatusOK)
}

func Status(s int) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	xhttpheader.SetContentType(resp.Headers, xmime.Plain)
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
	xhttpheader.SetContentType(resp.Headers, xmime.JSON)
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
	xhttpheader.SetContentType(resp.Headers, xmime.CSS)
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
	xhttpheader.SetContentType(resp.Headers, xmime.HtmlUTF8)
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
	xhttpheader.SetContentType(resp.Headers, xmime.Plain)
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
	resp.Headers[xhttpheader.KeyLocation] = location
	return resp
}

func buildContext(ctx context.Context, request *Request) context.Context {
	a := xcontext.NewActivity("", request.Headers)
	ctx = xcontext.WithIncomingActivity(ctx, a)
	traceID := a.GetTraceID()
	if traceID == "" {
		traceID = uuid.NewString()
		a.SetTraceID(traceID)
	}
	logger := xlog.FromContext(ctx).With(slog.String("trace_id", traceID))
	ctx = xlog.NewContext(ctx, logger)
	return ctx
}
