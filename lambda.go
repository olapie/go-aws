package awskit

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"code.olapie.com/log"
	"code.olapie.com/ola/httpkit"

	"code.olapie.com/router"
	"github.com/aws/aws-lambda-go/events"
)

type LambdaFunc func(ctx context.Context, request *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error)

type LambdaRouter struct {
	*router.Router[LambdaFunc]
}

func NewLambdaRouter() *LambdaRouter {
	return &LambdaRouter{
		Router: router.New[LambdaFunc](),
	}
}

func (r *LambdaRouter) Handle(ctx context.Context, request *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
	httpInfo := request.RequestContext.HTTP
	endpoint, _ := r.Match(httpInfo.Method, request.RawPath)
	if endpoint != nil {
		handler := endpoint.Handler()
		for handler != nil {
			resp, err := handler.Handler()(ctx, request)
			if err != nil {
				return APIErrorResponse(err), nil
			}
			if resp != nil {
				return resp, nil
			}
			handler = handler.Next()
		}
	}
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = http.StatusNotFound
	resp.Body = http.StatusText(http.StatusNotFound)
	return resp, nil
}

func CreateAPIRequestVerifier(pubKey *ecdsa.PublicKey) LambdaFunc {
	return func(ctx context.Context, request *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
		ts := httpkit.GetHeader(request.Headers, httpkit.KeyTimestamp)
		t, _ := conv.ToInt64(ts)
		now := time.Now().Unix()
		if conv.Abs(now-t) > 5 {
			return APIErrorResponse(errors.NotAcceptable("outdated request")), nil
		}

		message := request.RequestContext.HTTP.Method + request.RequestContext.HTTP.Path + GetAccessToken(request) + ts
		hash := sha256.Sum256([]byte(message))

		signature := httpkit.GetHeader(request.Headers, httpkit.KeySignature)
		sign, err := base64.StdEncoding.DecodeString(signature)
		if err != nil {
			log.S().Errorf("base64.DecodeString: signature=%s, %v", signature, err)
			return APIErrorResponse(errors.NotAcceptable("malformed signature")), nil
		}

		if ecdsa.VerifyASN1(pubKey, hash[:], sign) {
			return nil, nil
		}
		return APIErrorResponse(errors.NotAcceptable("invalid signature")), nil
	}
}

func APIErrorResponse(err error) *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.Plain
	if err == nil {
		resp.StatusCode = http.StatusOK
		resp.Body = http.StatusText(http.StatusOK)
		return resp
	}
	resp.StatusCode = errors.GetCode(err)
	resp.Body = err.Error()
	return resp
}

func APISuccessResponse() *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.Plain
	resp.StatusCode = http.StatusOK
	resp.Body = http.StatusText(http.StatusOK)
	return resp
}

func APIJsonResponse(v any) *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = http.StatusOK
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.JSON
	resp.Body = conv.MustJSONString(v)
	return resp
}

func GetAccessToken(request *events.APIGatewayV2HTTPRequest) string {
	accessToken := request.Headers[httpkit.KeyAuthorization]
	if accessToken != "" {
		return accessToken
	}

	accessToken = request.Headers[strings.ToLower(httpkit.KeyAuthorization)]
	if accessToken != "" {
		return accessToken
	}
	return request.QueryStringParameters["access_token"]
}
