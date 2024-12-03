package models

import (
	"context"
	"encoding/json"
	"fmt"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/conf"
	"github.com/ccfos/nightingale/v6/ibex/server/config"
	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/pkg/poster"
	"github.com/ccfos/nightingale/v6/storage"
)

func tht(id int64) string {
	return fmt.Sprintf("task_host_%d", id%100)
}

func TableRecordGets[T any](table, where string, args ...interface{}) (lst T, err error) {
	if config.C.IsCenter {
		finder := zorm.NewSelectFinder(table)
		if where == "" || len(args) == 0 {
		} else {
			models.AppendWhere(finder, where, args...)
		}
		err = zorm.Query(context.Background(), finder, &lst, nil)
		return lst, err
	}

	return poster.PostByUrlsWithResp[T](NewN9eCtx(config.C.CenterApi), "/ibex/v1/table/record/list", map[string]interface{}{
		"table": table,
		"where": where,
		"args":  args,
	})
}

func TableRecordCount(table, where string, args ...interface{}) (int64, error) {
	if config.C.IsCenter {
		finder := zorm.NewSelectFinder(table, "count(*)")
		finder.InjectionCheck = false
		if where == "" || len(args) == 0 {
		} else {
			models.AppendWhere(finder, where, args...)
		}

		return models.Count(&ctx.Context{Ctx: context.Background()}, finder)
		//return Count(DB().Table(table).Where(where, args...))
	}

	return poster.PostByUrlsWithResp[int64](NewN9eCtx(config.C.CenterApi), "/ibex/v1/table/record/count", map[string]interface{}{
		"table": table,
		"where": where,
		"args":  args,
	})
}

var IBEX_HOST_DOING string = "ibex-host-doing"

func CacheRecordGets[T any](ctx context.Context) ([]T, error) {
	lst := make([]T, 0)
	values, _ := storage.Cache.HVals(ctx, IBEX_HOST_DOING).Result()
	for _, val := range values {
		t := new(T)
		if err := json.Unmarshal([]byte(val), t); err != nil {
			return nil, err
		}
		lst = append(lst, *t)
	}
	return lst, nil
}

func NewN9eCtx(centerApi conf.CenterApi) *ctx.Context {
	return &ctx.Context{
		CenterApi: centerApi,
		Ctx:       context.Background(),
	}
}
