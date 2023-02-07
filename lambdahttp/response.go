package lambdahttp

import (
	"context"
	"net/http"

	"code.olapie.com/log"
	"code.olapie.com/sugar/v2/xcontext"
	"code.olapie.com/sugar/v2/xerror"
	"code.olapie.com/sugar/v2/xhttp"
	"code.olapie.com/sugar/v2/xjson"
	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
)

func Error(err error) *Response {
	if err == nil {
		return OK()
	}

	if er, ok := err.(*xerror.Error); ok {
		return JSON(er.Code(), er)
	}

	code := xerror.GetCode(err)
	if code == 0 {
		code = http.StatusInternalServerError
	}
	return JSON(code, xerror.New(code, err.Error()))
}

func OK() *Response {
	return Status(http.StatusOK)
}

func Status(s int) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[xhttp.KeyContentType] = xhttp.Plain
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
	resp.Headers[xhttp.KeyContentType] = xhttp.JSON
	resp.Body = xjson.ToString(v)
	return resp
}

func CSS200(cssText string) *Response {
	return CSS(http.StatusOK, cssText)
}

func CSS(status int, cssText string) *Response {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	resp.Headers[xhttp.KeyContentType] = xhttp.CSS
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
	resp.Headers[xhttp.KeyContentType] = xhttp.HtmlUTF8
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
	resp.Headers[xhttp.KeyContentType] = xhttp.Plain
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
	resp.Headers[xhttp.KeyLocation] = location
	return resp
}

func BuildContext(ctx context.Context, request *Request) context.Context {
	appID := xhttp.GetHeader(request.Headers, xhttp.KeyAppID)
	clientID := xhttp.GetHeader(request.Headers, xhttp.KeyClientID)
	traceID := xhttp.GetHeader(request.Headers, xhttp.KeyTraceID)
	if traceID == "" {
		traceID = uuid.NewString()
	}
	ctx = xcontext.WithRequestMetadata(ctx, xcontext.RequestMetadata{
		TraceID:  traceID,
		ClientID: clientID,
		AppID:    appID,
	})
	logger := log.FromContext(ctx).With(log.String("trace_id", traceID))
	ctx = log.BuildContext(ctx, logger)
	return ctx
}
