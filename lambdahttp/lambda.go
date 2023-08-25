package lambdahttp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"go.olapie.com/logs"
	"go.olapie.com/ola/activity"
	"go.olapie.com/ola/errorutil"
	"go.olapie.com/ola/headers"
	"go.olapie.com/router"
	"go.olapie.com/types"
)

type Request = events.APIGatewayV2HTTPRequest
type Response = events.APIGatewayV2HTTPResponse
type Func = router.HandlerFunc[*Request, *Response]

type Router struct {
	*router.Router[Func]
	verifyAPIKey func(header map[string]string) bool
	authenticate func(ctx context.Context, accessToken string) (int64, error)
}

func NewRouter(verifyAPIKey func(header map[string]string) bool, authenticate func(ctx context.Context, accessToken string) (int64, error)) *Router {
	return &Router{
		Router:       router.New[Func](),
		verifyAPIKey: verifyAPIKey,
		authenticate: authenticate,
	}
}

func (r *Router) Handle(ctx context.Context, request *Request) (resp *Response) {
	ctx = buildContext(ctx, request)
	httpInfo := request.RequestContext.HTTP
	logger := logs.FromContext(ctx)
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

		logger = logs.FromContext(ctx).With(slog.Int("status_code", resp.StatusCode))
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
		resp.Headers[headers.KeyTraceID] = activity.FromIncomingContext(ctx).GetTraceID()
	}()

	if r.verifyAPIKey != nil && !r.verifyAPIKey(request.Headers) {
		return Error(errorutil.BadRequest("invalid api key"))
	}

	if r.authenticate != nil {
		accessToken := headers.GetBearer(request.Headers)
		if accessToken == "" {
			accessToken = headers.GetAuthorization(request.Headers)
		}
		if accessToken == "" {
			logger.Warn("missing access token")
		} else {
			uid, err := r.authenticate(ctx, accessToken)
			if err != nil {
				Error(errorutil.Unauthorized(err.Error()))
			}

			if uid > 0 {
				_ = activity.SetIncomingUserID(ctx, uid)
				logger.Info("authenticated", slog.Int64("uid", uid))
			}
		}
	}

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
