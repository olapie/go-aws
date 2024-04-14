package lambdahttp

import (
	"testing"

	"go.olapie.com/x/xerror"
)

func TestJSON(t *testing.T) {
	err := xerror.BadRequest("test")
	body := Error(err).Body
	t.Log(err)
	t.Log(body)
	t.Log(body[0:1])
}
