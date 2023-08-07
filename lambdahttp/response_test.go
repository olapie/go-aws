package lambdahttp

import (
	"testing"

	"go.olapie.com/ola/errorutil"
)

func TestJSON(t *testing.T) {
	err := errorutil.BadRequest("test")
	body := Error(err).Body
	t.Log(err)
	t.Log(body)
	t.Log(body[0:1])
}
