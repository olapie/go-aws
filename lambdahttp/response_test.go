package lambdahttp

import (
	"go.olapie.com/rpcx/http"
	"testing"
)

func TestJSON(t *testing.T) {
	err := http.BadRequest("test")
	body := Error(err).Body
	t.Log(err)
	t.Log(body)
	t.Log(body[0:1])
}
