package apigateway

import (
	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"code.olapie.com/ola/httpkit"
	"github.com/aws/aws-lambda-go/events"
	"net/http"
	"strings"
)

func Error(err error) *events.APIGatewayV2HTTPResponse {
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

func OK() *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.Plain
	resp.StatusCode = http.StatusOK
	resp.Body = http.StatusText(http.StatusOK)
	return resp
}

func JSON(v any) *events.APIGatewayV2HTTPResponse {
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
