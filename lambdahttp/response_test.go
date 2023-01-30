package lambdahttp

import (
	"code.olapie.com/sugar/v2/xerror"
	"code.olapie.com/sugar/v2/xjson"
	"testing"
)

func TestJSON(t *testing.T) {
	err := xerror.BadRequest("test")
	body := Error(err).Body
	t.Log(xjson.ToString(err))
	t.Log(body)
	t.Log(body[0:1])
}
