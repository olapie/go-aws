package lambdahttp

import (
	"net/http"
	"testing"

	"code.olapie.com/sugar/v2/jsonutil"
	"code.olapie.com/sugar/v2/xerror"
)

func TestJSON(t *testing.T) {
	err := xerror.New(http.StatusBadRequest, "test")
	body := Error(err).Body
	t.Log(jsonutil.ToString(err))
	t.Log(body)
	t.Log(body[0:1])
}
