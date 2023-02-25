package lambdahttp

import (
	"context"
	"errors"
	"fmt"

	"code.olapie.com/sugar/v2/httperror"

	"code.olapie.com/log"
	"code.olapie.com/router"
	"code.olapie.com/sugar/v2/ctxutil"
	"code.olapie.com/sugar/v2/httpheader"
	"code.olapie.com/sugar/v2/jsonutil"
	"github.com/aws/aws-lambda-go/events"
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
	logger := log.FromContext(ctx)
	logger.Info("Start",
		log.String("header", jsonutil.ToString(request.Headers)),
		log.String("path", request.RawPath),
		log.String("query", request.RawQueryString),
		log.String("method", httpInfo.Method),
		log.String("user_agent", httpInfo.UserAgent),
		log.String("source_ip", httpInfo.SourceIP),
	)

	defer func() {
		if msg := recover(); msg != nil {
			logger.Error("Panic", log.Any("error", msg))
			resp = Error(errors.New(fmt.Sprint(msg)))
			return
		}

		logger := log.FromContext(ctx).With(log.Int("status_code", resp.StatusCode))
		if resp.StatusCode < 400 {
			logger.Info("End")
		} else {
			if len(resp.Body) < 1024 {
				logger.Error("End", log.String("body", resp.Body))
			} else {
				logger.Error("End")
			}
		}

		if resp.Headers == nil {
			resp.Headers = make(map[string]string)
		}
		httpheader.SetTraceID(resp.Headers, ctxutil.GetTraceID(ctx))
	}()

	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		resp = handler.Handler()(ctx, request)
		if resp == nil {
			resp = Error(httperror.NotImplemented("no response from handler"))
		}
		return resp
	}
	return Error(httperror.NotFound("endpoint not found: %s %s", httpInfo.Method, request.RawPath))
}

func Next(ctx context.Context, request *Request) *Response {
	return router.Next[*Request, *Response](ctx, request)
}
