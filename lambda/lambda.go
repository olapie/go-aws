package lambda

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"code.olapie.com/log"
	"code.olapie.com/router"
	"code.olapie.com/sugar/contexts"
	"code.olapie.com/sugar/conv"
	"code.olapie.com/sugar/errorx"
	"code.olapie.com/sugar/httpx"
	"code.olapie.com/sugar/jsonx"
	"code.olapie.com/sugar/mathx"
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
	traceID := contexts.GetTraceID(ctx)

	defer func() {
		if msg := recover(); msg != nil {
			logger.Error("caught a panic", log.Any("error", msg))
			resp = Error(errors.New(fmt.Sprint(msg)))
			return
		}

		if resp.StatusCode < 400 {
			log.FromContext(ctx).Info("succeeded", log.Int("status_code", resp.StatusCode))
		} else {
			log.FromContext(ctx).Error("failed", log.Int("status_code", resp.StatusCode),
				log.String("body", resp.Body))
		}
		if resp.Headers == nil {
			resp.Headers = make(map[string]string)
		}
		httpx.SetTraceID(resp.Headers, traceID)
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
		ts := httpx.GetHeader(request.Headers, httpx.KeyTimestamp)
		t, _ := conv.ToInt64(ts)
		now := time.Now().Unix()
		if mathx.Abs(now-t) > 5 {
			return Error(errorx.NotAcceptable("outdated request"))
		}

		authorization := httpx.GetHeader(request.Headers, httpx.KeyAuthorization)
		message := request.RequestContext.HTTP.Method + request.RequestContext.HTTP.Path + authorization + ts
		hash := sha256.Sum256([]byte(message))

		signature := httpx.GetHeader(request.Headers, httpx.KeySignature)
		sign, err := base64.StdEncoding.DecodeString(signature)
		if err != nil {
			log.S().Errorf("base64.DecodeString: signature=%s, %v", signature, err)
			return Error(errorx.NotAcceptable("malformed signature"))
		}

		if ecdsa.VerifyASN1(pubKey, hash[:], sign) {
			return Next(ctx, request)
		}
		return Error(errorx.NotAcceptable("invalid signature"))
	}
}

func Next(ctx context.Context, request *Request) *Response {
	return router.Next[*Request, *Response](ctx, request)
}
