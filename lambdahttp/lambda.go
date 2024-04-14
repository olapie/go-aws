package lambdahttp

import (
	"context"
	"errors"
	"fmt"
	"go.olapie.com/ola/types"
	"go.olapie.com/x/xcontext"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"go.olapie.com/ola/activity"
	"go.olapie.com/router"
	"go.olapie.com/x/xerror"
	"go.olapie.com/x/xhttpheader"
	"go.olapie.com/x/xlog"
)

type Request = events.APIGatewayV2HTTPRequest
type Response = events.APIGatewayV2HTTPResponse
type Func = router.HandlerFunc[*Request, *Response]

type Router struct {
	*router.Router[Func]
	verifyAPIKey func(ctx context.Context, header map[string]string) bool
	authenticate func(ctx context.Context, xhttpheader map[string]string) types.UserID
}

func NewRouter(verifyAPIKey func(ctx context.Context, header map[string]string) bool, authenticate func(ctx context.Context, xhttpheader map[string]string) types.UserID) *Router {
	return &Router{
		Router:       router.New[Func](),
		verifyAPIKey: verifyAPIKey,
		authenticate: authenticate,
	}
}

func (r *Router) Handle(ctx context.Context, request *Request) (resp *Response) {
	ctx = buildContext(ctx, request)
	httpInfo := request.RequestContext.HTTP
	logger := xlog.FromContext(ctx)
	logger.Info("START",
		slog.Any("header", request.Headers),
		slog.String("path", request.RawPath),
		slog.String("query", request.RawQueryString),
		slog.String("method", httpInfo.Method),
		slog.String("user_agent", httpInfo.UserAgent),
		slog.String("source_ip", httpInfo.SourceIP),
	)

	defer func() {
		if msg := recover(); msg != nil {
			logger.Error("PANIC", slog.Any("error", msg))
			resp = Error(errors.New(fmt.Sprint(msg)))
			return
		}

		logger = xlog.FromContext(ctx).With(slog.Int("status_code", resp.StatusCode))
		if resp.StatusCode < 400 {
			logger.Info("End")
		} else {
			if len(resp.Body) < 1024 {
				logger.Error("END", slog.String("body", resp.Body))
			} else {
				logger.Error("END")
			}
		}

		if resp.Headers == nil {
			resp.Headers = make(map[string]string)
		}
		resp.Headers[xhttpheader.KeyTraceID] = activity.FromIncomingContext(ctx).GetTraceID()
	}()

	if r.verifyAPIKey != nil && !r.verifyAPIKey(ctx, request.Headers) {
		return Error(xerror.BadRequest("invalid api key"))
	}

	if r.authenticate != nil {
		uid := r.authenticate(ctx, request.Headers)
		if uid != nil {
			xcontext.GetIncomingActivity(ctx).SetUserID(uid)
			logger.Info("authenticated", slog.Any("uid", uid.Value()))
		}
	}

	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		resp = handler.Handler()(ctx, request)
		if resp == nil {
			resp = Error(xerror.NotImplemented("no response from handler"))
		}
		return resp
	}
	return Error(xerror.NotFound("endpoint not found: %s %s", httpInfo.Method, request.RawPath))
}

func Next(ctx context.Context, request *Request) *Response {
	return router.Next[*Request, *Response](ctx, request)
}
