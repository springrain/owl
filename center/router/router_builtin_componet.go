package router

import (
	"context"
	"net/http"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/models"

	"github.com/gin-gonic/gin"
	"github.com/toolkits/pkg/ginx"
)

const SYSTEM = "system"

func (rt *Router) builtinComponentsAdd(c *gin.Context) {
	var lst []models.BuiltinComponent
	ginx.BindJSON(c, &lst)

	username := Username(c)

	count := len(lst)
	if count == 0 {
		ginx.Bomb(http.StatusBadRequest, "input json is empty")
	}

	reterr := make(map[string]string)
	for i := 0; i < count; i++ {
		if err := lst[i].Add(rt.Ctx, username); err != nil {
			reterr[lst[i].Ident] = err.Error()
		}
	}

	ginx.NewRender(c).Data(reterr, nil)
}

func (rt *Router) builtinComponentsGets(c *gin.Context) {
	query := ginx.QueryStr(c, "query", "")

	bc, err := models.BuiltinComponentGets(rt.Ctx, query)
	ginx.Dangerous(err)

	ginx.NewRender(c).Data(bc, nil)
}

func (rt *Router) builtinComponentsPut(c *gin.Context) {
	var req models.BuiltinComponent
	ginx.BindJSON(c, &req)

	bc, err := models.BuiltinComponentGet(rt.Ctx, "id = ?", req.ID)
	ginx.Dangerous(err)

	if bc == nil {
		ginx.NewRender(c, http.StatusNotFound).Message("No such builtin component")
		return
	}

	if bc.CreatedBy == SYSTEM {
		req.Ident = bc.Ident
	}

	username := Username(c)
	req.UpdatedBy = username

	_, err = zorm.Transaction(rt.Ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		rt.Ctx.Ctx = ctx
		txErr := models.BuiltinMetricBatchUpdateColumn(rt.Ctx, "typ", bc.Ident, req.Ident, req.UpdatedBy)
		if txErr != nil {
			return nil, txErr
		}
		txErr = bc.Update(rt.Ctx, req)
		if txErr != nil {
			return nil, txErr
		}
		return nil, txErr
	})

	ginx.NewRender(c).Message(err)
}

func (rt *Router) builtinComponentsDel(c *gin.Context) {
	var req idsForm
	ginx.BindJSON(c, &req)

	req.Verify()

	ginx.NewRender(c).Message(models.BuiltinComponentDels(rt.Ctx, req.Ids))
}
