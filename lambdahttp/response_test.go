package lambdahttp

import (
	"go.olapie.com/rpcx/httpx"
	"testing"
)

func TestJSON(t *testing.T) {
	err := httpx.BadRequest("test")
	body := Error(err).Body
	t.Log(err)
	t.Log(body)
	t.Log(body[0:1])
}
