package lambda

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"time"

	"code.olapie.com/awskit/apigateway"
	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"code.olapie.com/log"
	"code.olapie.com/ola/httpkit"
	"code.olapie.com/router"
	"github.com/aws/aws-lambda-go/events"
)

type Func func(ctx context.Context, request *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error)

type Router struct {
	*router.Router[Func]
}

func NewRouter() *Router {
	return &Router{
		Router: router.New[Func](),
	}
}

func (r *Router) Handle(ctx context.Context, request *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
	httpInfo := request.RequestContext.HTTP
	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		for handler != nil {
			resp, err := handler.Handler()(ctx, request)
			if err != nil {
				log.FromContext(ctx).Sugar().Error(err)
				return apigateway.Error(err), nil
			}
			if resp != nil {
				return resp, nil
			}
			handler = handler.Next()
		}
	}
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = http.StatusNotImplemented
	resp.Body = "Not implemented: " + httpInfo.Method + " " + request.RawPath
	return resp, nil
}

func CreateRequestVerifier(pubKey *ecdsa.PublicKey) Func {
	return func(ctx context.Context, request *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
		ts := httpkit.GetHeader(request.Headers, httpkit.KeyTimestamp)
		t, _ := conv.ToInt64(ts)
		now := time.Now().Unix()
		if conv.Abs(now-t) > 5 {
			return apigateway.Error(errors.NotAcceptable("outdated request")), nil
		}

		authorization := httpkit.GetHeader(request.Headers, httpkit.KeyAuthorization)
		message := request.RequestContext.HTTP.Method + request.RequestContext.HTTP.Path + authorization + ts
		hash := sha256.Sum256([]byte(message))

		signature := httpkit.GetHeader(request.Headers, httpkit.KeySignature)
		sign, err := base64.StdEncoding.DecodeString(signature)
		if err != nil {
			log.S().Errorf("base64.DecodeString: signature=%s, %v", signature, err)
			return apigateway.Error(errors.NotAcceptable("malformed signature")), nil
		}

		if ecdsa.VerifyASN1(pubKey, hash[:], sign) {
			return nil, nil
		}
		return apigateway.Error(errors.NotAcceptable("invalid signature")), nil
	}
}
