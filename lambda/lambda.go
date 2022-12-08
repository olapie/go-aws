package lambda

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/md5"
	"errors"
	"fmt"

	"code.olapie.com/log"
	"code.olapie.com/router"
	"code.olapie.com/sugar/contexts"
	"code.olapie.com/sugar/errorx"
	"code.olapie.com/sugar/httpx"
	"code.olapie.com/sugar/jsonx"
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
	logger.Info("received",
		log.String("header", jsonx.ToString(request.Headers)),
		log.String("path", request.RawPath),
		log.String("query", request.RawQueryString),
		log.String("method", httpInfo.Method),
		log.String("user_agent", httpInfo.UserAgent),
		log.String("source_ip", httpInfo.SourceIP),
	)

	defer func() {
		if msg := recover(); msg != nil {
			logger.Error("caught a panic", log.Any("error", msg))
			resp = Error(errors.New(fmt.Sprint(msg)))
			return
		}

		logger := log.FromContext(ctx).With(log.Int("status_code", resp.StatusCode))
		if resp.StatusCode < 400 {
			logger.Info("succeeded")
		} else {
			if len(resp.Body) < 1024 {
				logger.Error("failed", log.String("body", resp.Body))
			} else {
				logger.Error("failed")
			}
		}

		if resp.Headers == nil {
			resp.Headers = make(map[string]string)
		}
		httpx.SetTraceID(resp.Headers, contexts.GetTraceID(ctx))
	}()

	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		resp = handler.Handler()(ctx, request)
		if resp == nil {
			resp = Error(errorx.NotImplemented("no response from handler"))
		}
		return resp
	}
	return Error(errorx.NotFound("endpoint not found"))
}

func CreateRequestVerifier(pubKey *ecdsa.PublicKey) Func {
	return func(ctx context.Context, request *Request) *Response {
		if err := httpx.CheckTimestamp(request.Headers); err != nil {
			return Error(err)
		}
		sign, err := httpx.DecodeSign(request.Headers)
		if err != nil {
			return Error(err)
		}
		hash := getMessageHashForSigning(ctx, request)
		if ecdsa.VerifyASN1(pubKey, hash[:], sign) {
			return Next(ctx, request)
		}
		return Error(errorx.NotAcceptable("invalid signature"))
	}
}

func getMessageHashForSigning(ctx context.Context, req *Request) []byte {
	httpInfo := req.RequestContext.HTTP
	var buf bytes.Buffer
	buf.WriteString(httpInfo.Method)
	buf.WriteString(httpInfo.Path)
	buf.WriteString(req.RawQueryString)
	buf.WriteString(httpx.GetHeader(req.Headers, httpx.KeyTraceID))
	buf.WriteString(httpx.GetHeader(req.Headers, httpx.KeyTimestamp))
	hash := md5.Sum(buf.Bytes())
	return hash[:]
}

func Next(ctx context.Context, request *Request) *Response {
	return router.Next[*Request, *Response](ctx, request)
}
