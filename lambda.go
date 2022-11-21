package awskit

import (
	"context"

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
