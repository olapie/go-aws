package apigateway

import (
	"code.olapie.com/conv"
	"code.olapie.com/errors"
	"code.olapie.com/ola/httpkit"
	"github.com/aws/aws-lambda-go/events"
	"net/http"
)

func Error(err error) *events.APIGatewayV2HTTPResponse {
	if err == nil {
		return OK()
	}

	if er, ok := err.(*errors.Error); ok {
		return JSON(er.Code, er)
	}

	var er errors.Error
	er.Code = errors.GetCode(err)
	if er.Code == 0 {
		er.Code = http.StatusInternalServerError
	}
	er.Message = err.Error()
	return JSON(er.Code, er)
}

func OK() *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.Plain
	resp.StatusCode = http.StatusOK
	resp.Body = http.StatusText(http.StatusOK)
	return resp
}

func NoContent() *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = http.StatusNoContent
	return resp
}

func JSON200(v any) *events.APIGatewayV2HTTPResponse {
	return JSON(200, v)
}

func JSON201(v any) *events.APIGatewayV2HTTPResponse {
	return JSON(201, v)
}

func JSON202(v any) *events.APIGatewayV2HTTPResponse {
	return JSON(202, v)
}

func JSON(status int, v any) *events.APIGatewayV2HTTPResponse {
	resp := new(events.APIGatewayV2HTTPResponse)
	resp.StatusCode = status
	resp.Headers = make(map[string]string)
	resp.Headers[httpkit.KeyContentType] = httpkit.JSON
	resp.Body = conv.MustJSONString(v)
	return resp
}
