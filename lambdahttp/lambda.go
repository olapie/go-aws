package lambdahttp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"go.olapie.com/logs"
	"go.olapie.com/ola/activity"
	"go.olapie.com/ola/headers"
	"go.olapie.com/router"
	"go.olapie.com/types"
)

type Request = events.APIGatewayV2HTTPRequest
type Response = events.APIGatewayV2HTTPResponse
type Func = router.HandlerFunc[*Request, *Response]

type Router struct {
	*router.Router[Func]
}

func NewRouter() *Router {
	return &Router{
		Router: router.New[Func](),
	}
}

func (r *Router) Handle(ctx context.Context, request *Request) (resp *Response) {
	ctx = BuildContext(ctx, request)
	httpInfo := request.RequestContext.HTTP
	logger := logs.FromCtx(ctx)
	logger.Info("Start",
		slog.Any("header", request.Headers),
		slog.String("path", request.RawPath),
		slog.String("query", request.RawQueryString),
		slog.String("method", httpInfo.Method),
		slog.String("user_agent", httpInfo.UserAgent),
		slog.String("source_ip", httpInfo.SourceIP),
	)

	defer func() {
		if msg := recover(); msg != nil {
			logger.Error("Panic", slog.Any("error", msg))
			resp = Error(errors.New(fmt.Sprint(msg)))
			return
		}

		logger := logs.FromCtx(ctx).With(slog.Int("status_code", resp.StatusCode))
		if resp.StatusCode < 400 {
			logger.Info("End")
		} else {
			if len(resp.Body) < 1024 {
				logger.Error("End", slog.String("body", resp.Body))
			} else {
				logger.Error("End")
			}
		}

		if resp.Headers == nil {
			resp.Headers = make(map[string]string)
		}
		resp.Headers[headers.KeyTraceID] = activity.FromIncomingContext(ctx).Get(headers.KeyTraceID)
	}()

	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		resp = handler.Handler()(ctx, request)
		if resp == nil {
			resp = Error(types.NotImplemented("no response from handler"))
		}
		return resp
	}
	return Error(types.NotFound("endpoint not found: %s %s", httpInfo.Method, request.RawPath))
}

func Next(ctx context.Context, request *Request) *Response {
	return router.Next[*Request, *Response](ctx, request)
}
