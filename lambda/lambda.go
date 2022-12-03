package lambda

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"code.olapie.com/awskit/apigateway"
	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"code.olapie.com/log"
	"code.olapie.com/ola/httpkit"
	"code.olapie.com/router"
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
	ctx = apigateway.BuildContext(ctx, request)
	httpInfo := request.RequestContext.HTTP
	logger := log.FromContext(ctx)
	logger.Debug("handle request",
		log.String("header", conv.MustJSONString(request.Headers)),
		log.String("path", request.RawPath),
		log.String("query", request.RawQueryString),
		log.String("method", httpInfo.Method),
		log.String("user_agent", httpInfo.UserAgent),
		log.String("source_ip", httpInfo.SourceIP),
		log.String("http_path", httpInfo.Path),
	)

	defer func() {
		if msg := recover(); msg != nil {
			logger.Error("caught a panic", log.Any("error", msg))
			resp = apigateway.Error(errors.New(fmt.Sprint(msg)))
			return
		}

		if resp.StatusCode < 400 {
			log.FromContext(ctx).Info("Finished", log.Int("status_code", resp.StatusCode))
		} else {
			log.FromContext(ctx).Error("Failed", log.Int("status_code", resp.StatusCode),
				log.String("body", resp.Body))
		}
	}()

	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		ctx = router.WithNextHandler(ctx, handler.Next())
		resp = handler.Handler()(ctx, request)
		if resp == nil {
			resp = apigateway.Error(errors.NotImplemented("no response from handler"))
		}
		return resp
	}
	return apigateway.Error(errors.NotFound("endpoint not found"))
}

func CreateRequestVerifier(pubKey *ecdsa.PublicKey) Func {
	return func(ctx context.Context, request *Request) *Response {
		ts := httpkit.GetHeader(request.Headers, httpkit.KeyTimestamp)
		t, _ := conv.ToInt64(ts)
		now := time.Now().Unix()
		if conv.Abs(now-t) > 5 {
			return apigateway.Error(errors.NotAcceptable("outdated request"))
		}

		authorization := httpkit.GetHeader(request.Headers, httpkit.KeyAuthorization)
		message := request.RequestContext.HTTP.Method + request.RequestContext.HTTP.Path + authorization + ts
		hash := sha256.Sum256([]byte(message))

		signature := httpkit.GetHeader(request.Headers, httpkit.KeySignature)
		sign, err := base64.StdEncoding.DecodeString(signature)
		if err != nil {
			log.S().Errorf("base64.DecodeString: signature=%s, %v", signature, err)
			return apigateway.Error(errors.NotAcceptable("malformed signature"))
		}

		if ecdsa.VerifyASN1(pubKey, hash[:], sign) {
			return nil
		}
		return apigateway.Error(errors.NotAcceptable("invalid signature"))
	}
}

func Next(ctx context.Context, request *Request) *Response {
	return router.Next[*Request, *Response](ctx, request)
}
