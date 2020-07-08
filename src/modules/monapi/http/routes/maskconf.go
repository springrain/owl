package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/toolkits/pkg/errors"

	"github.com/didi/nightingale/src/model"
)

type MaskconfForm struct {
	Nid         int64             `json:"nid"`
	Category    int               `json:"category"` //1 设备相关 2 设备无关
	Endpoints   []string          `json:"endpoints"`
	CurNidPaths map[string]string `json:"cur_nid_paths"`
	Metric      string            `json:"metric"`
	Tags        string            `json:"tags"`
	Cause       string            `json:"cause"`
	Btime       int64             `json:"btime"`
	Etime       int64             `json:"etime"`
}

func (f MaskconfForm) Validate() {
	mustNode(f.Nid)

	if f.Category == 1 && (f.Endpoints == nil || len(f.Endpoints) == 0) {
		errors.Bomb("arg[endpoints] empty")
	}

	if f.Category == 2 && len(f.CurNidPaths) == 0 {
		errors.Bomb("arg[cur_nid_paths] empty")
	}

	if f.Btime >= f.Etime {
		errors.Bomb("args[btime,etime] invalid")
	}
}

func maskconfPost(c *gin.Context) {
	var f MaskconfForm
	errors.Dangerous(c.ShouldBind(&f))
	f.Validate()

	obj := &model.Maskconf{
		Nid:    f.Nid,
		Metric: f.Metric,
		Tags:   f.Tags,
		Cause:  f.Cause,
		Btime:  f.Btime,
		Etime:  f.Etime,
		User:   loginUsername(c),
	}

	if f.Category == 1 {
		errors.Dangerous(obj.AddEndpoints(f.Endpoints))
	} else {
		errors.Dangerous(obj.AddNids(f.CurNidPaths))
	}

	renderMessage(c, nil)
}

func maskconfGets(c *gin.Context) {
	nid := urlParamInt64(c, "id")

	objs, err := model.MaskconfGets(nid)
	errors.Dangerous(err)

	for i := 0; i < len(objs); i++ {
		if objs[i].Category == 1 {
			errors.Dangerous(objs[i].FillEndpoints())
		} else {
			errors.Dangerous(objs[i].FillNids())
		}
	}

	renderData(c, objs, nil)
}

func maskconfDel(c *gin.Context) {
	id := urlParamInt64(c, "id")
	renderMessage(c, model.MaskconfDel(id))
}

func maskconfPut(c *gin.Context) {
	mc, err := model.MaskconfGet("id", urlParamInt64(c, "id"))
	errors.Dangerous(err)

	if mc == nil {
		errors.Bomb("maskconf is nil")
	}

	var f MaskconfForm
	errors.Dangerous(c.ShouldBind(&f))
	f.Validate()

	mc.Metric = f.Metric
	mc.Tags = f.Tags
	mc.Etime = f.Etime
	mc.Btime = f.Btime
	mc.Cause = f.Cause
	mc.Category = f.Category

	if f.Category == 1 {
		renderMessage(c, mc.UpdateEndpoints(f.Endpoints, "metric", "tags", "etime", "btime", "cause"))
	} else {
		renderMessage(c, mc.UpdateNids(f.CurNidPaths, "metric", "tags", "etime", "btime", "cause"))
	}
}
