package models

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/ibex/server/config"
	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/poster"
	"github.com/ccfos/nightingale/v6/storage"

	"github.com/toolkits/pkg/logger"
)

const TaskHostTableName = "task_host"

type TaskHost struct {
	zorm.EntityStruct
	Id     int64  `column:"id" json:"id"`
	II     int64  `column:"ii" json:"-"`
	Host   string `column:"host" json:"host"`
	Status string `column:"status" json:"status"`
	Stdout string `column:"stdout" json:"stdout"`
	Stderr string `column:"stderr" json:"stderr"`
}

func (taskHost *TaskHost) GetTableName() string {
	return tht(taskHost.Id)
}

func (taskHost *TaskHost) Upsert() error {

	f1 := zorm.NewSelectFinder(taskHost.GetTableName(), "id").Append("WHERE id=? and host=?", taskHost.Id, taskHost.Host)
	id := ""
	has, err := zorm.QueryRow(context.Background(), f1, &id)
	if err != nil {
		return err
	}
	if has || id != "" { //存在就更新
		finder := zorm.NewUpdateFinder(taskHost.GetTableName()).Append("status=?,stdout=?,stderr=? WHERE id=? and host=?", taskHost.Status, taskHost.Stdout, taskHost.Stderr, taskHost.Id, taskHost.Host)
		return models.UpdateFinder(NewN9eCtx(config.C.CenterApi), finder)
	}

	return models.Insert(NewN9eCtx(config.C.CenterApi), taskHost)

	/*
		return DB().Table(tht(taskHost.Id)).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}, {Name: "host"}},
			DoUpdates: clause.AssignmentColumns([]string{"status", "stdout", "stderr"}),
		}).Create(taskHost).Error
	*/
}

func (taskHost *TaskHost) Create() error {
	if config.C.IsCenter {
		return models.Insert(NewN9eCtx(config.C.CenterApi), taskHost)
		//return DB().Table(tht(taskHost.Id)).Create(taskHost).Error
	}
	return poster.PostByUrls(NewN9eCtx(config.C.CenterApi), "/ibex/v1/task/host", taskHost)
}

func TaskHostUpserts(lst []TaskHost) (map[string]error, error) {
	if len(lst) == 0 {
		return nil, fmt.Errorf("empty list")
	}

	if !config.C.IsCenter {
		return poster.PostByUrlsWithResp[map[string]error](NewN9eCtx(config.C.CenterApi), "/ibex/v1/task/hosts/upsert", lst)
	}

	errs := make(map[string]error, 0)
	for _, taskHost := range lst {
		if err := taskHost.Upsert(); err != nil {
			errs[fmt.Sprintf("%d:%s", taskHost.Id, taskHost.Host)] = err
		}
	}
	return errs, nil
}

func TaskHostGet(id int64, host string) (*TaskHost, error) {
	ret := make([]*TaskHost, 0)
	finder := zorm.NewSelectFinder(tht(id)).Append("WHERE id=? and host=?", id, host)
	err := zorm.Query(context.Background(), finder, &ret, nil)
	//err := DB().Table(tht(id)).Where("id=? and host=?", id, host).Find(&ret).Error
	if err != nil {
		return nil, err
	}

	if len(ret) == 0 {
		return nil, nil
	}

	return ret[0], nil
}

func MarkDoneStatus(id, clock int64, host, status, stdout, stderr string, edgeAlertTriggered ...bool) error {
	if len(edgeAlertTriggered) > 0 && edgeAlertTriggered[0] {
		return CacheMarkDone(context.Background(), TaskHost{
			Id:     id,
			Host:   host,
			Status: status,
			Stdout: stdout,
			Stderr: stderr,
		})
	}

	if !config.C.IsCenter {
		return poster.PostByUrls(NewN9eCtx(config.C.CenterApi), "/ibex/v1/mark/done", map[string]interface{}{
			"id":     id,
			"clock":  clock,
			"host":   host,
			"status": status,
			"stdout": stdout,
			"stderr": stderr,
		})
	}

	count, err := TableRecordCount(TaskHostDoing{}.GetTableName(), "id=? and host=? and clock=?", id, host, clock)
	if err != nil {
		return err
	}

	if count == 0 {
		// 如果是timeout了，后来任务执行完成之后，结果又上来了，stdout和stderr最好还是存库，让用户看到
		count, err = TableRecordCount(tht(id), "id=? and host=? and status=?", id, host, "timeout")
		if err != nil {
			return err
		}

		if count == 1 {

			finder := zorm.NewUpdateFinder(tht(id)).Append("status=?,stdout=?,stderr=? WHERE id=? and host=?", status, stdout, stderr, id, host)

			return models.UpdateFinder(NewN9eCtx(config.C.CenterApi), finder)

			/*
				return DB().Table(tht(id)).Where("id=? and host=?", id, host).Updates(map[string]interface{}{
					"status": status,
					"stdout": stdout,
					"stderr": stderr,
				}).Error
			*/
		}
		return nil

	}

	_, err = zorm.Transaction(context.Background(), func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewUpdateFinder(tht(id)).Append("status=?,stdout=?,stderr=? WHERE id=? and host=?", status, stdout, stderr, id, host)
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder2 := zorm.NewDeleteFinder(TaskHostDoingTableName).Append("WHERE id=? and host=?", id, host)
		return zorm.UpdateFinder(ctx, finder2)

	})
	return err
	/*
		return DB().Transaction(func(tx *gorm.DB) error {
			err = tx.Table(tht(id)).Where("id=? and host=?", id, host).Updates(map[string]interface{}{
				"status": status,
				"stdout": stdout,
				"stderr": stderr,
			}).Error
			if err != nil {
				return err
			}

			if err = tx.Where("id=? and host=?", id, host).Delete(&TaskHostDoing{}).Error; err != nil {
				return err
			}

			return nil
		})
	*/
}

func CacheMarkDone(ctx context.Context, taskHost TaskHost) error {
	if err := storage.Cache.HDel(ctx, IBEX_HOST_DOING, hostDoingCacheKey(taskHost.Id, taskHost.Host)).Err(); err != nil {
		return err
	}
	TaskHostCachePush(taskHost)

	return nil
}

func WaitingHostList(id int64, limit ...int) ([]TaskHost, error) {
	hosts := make([]TaskHost, 0)
	finder := zorm.NewSelectFinder(tht(id)).Append("WHERE id=? and status=? order by ii asc", id, "waiting")
	page := zorm.NewPage()
	if len(limit) > 0 {
		page.PageSize = limit[0]
	}

	err := zorm.Query(context.Background(), finder, &hosts, page)

	/*
		session := DB().Table(tht(id)).Where("id = ? and status = 'waiting'", id).Order("ii")
		if len(limit) > 0 {
			session = session.Limit(limit[0])
		}
		err := session.Find(&hosts).Error
	*/

	return hosts, err
}

func WaitingHostCount(id int64) (int64, error) {
	return TableRecordCount(tht(id), "id=? and status='waiting'", id)
}

func UnexpectedHostCount(id int64) (int64, error) {
	return TableRecordCount(tht(id), "id=? and status in ('failed', 'timeout', 'killfailed')", id)
}

func IngStatusHostCount(id int64) (int64, error) {
	return TableRecordCount(tht(id), "id=? and status in ('waiting', 'running', 'killing')", id)
}

func RunWaitingHosts(taskHosts []TaskHost) error {
	count := len(taskHosts)
	if count == 0 {
		return nil
	}

	now := time.Now().Unix()

	_, err := zorm.Transaction(context.Background(), func(ctx context.Context) (interface{}, error) {

		for i := 0; i < count; i++ {

			finder := zorm.NewUpdateFinder(tht(taskHosts[i].Id)).Append("status=? WHERE id=? and host=?", "running", taskHosts[i].Id, taskHosts[i].Host)
			_, err := zorm.UpdateFinder(ctx, finder)
			if err != nil {
				return nil, err
			}

			_, err = zorm.Insert(ctx, &TaskHostDoing{Id: taskHosts[i].Id, Host: taskHosts[i].Host, Clock: now, Action: "start"})
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	return err

	/*
		return DB().Transaction(func(tx *gorm.DB) error {
			for i := 0; i < count; i++ {
				if err := tx.Table(tht(taskHosts[i].Id)).Where("id=? and host=?", taskHosts[i].Id, taskHosts[i].Host).Update("status", "running").Error; err != nil {
					return err
				}
				err := tx.Create(&TaskHostDoing{Id: taskHosts[i].Id, Host: taskHosts[i].Host, Clock: now, Action: "start"}).Error
				if err != nil {
					return err
				}
			}

			return nil
		})
	*/
}

func TaskHostStatus(id int64) ([]TaskHost, error) {
	ret := make([]TaskHost, 0)
	finder := zorm.NewSelectFinder(tht(id), "id,host,status").Append("WHERE id=? order by ii asc", id)
	err := zorm.Query(context.Background(), finder, &ret, nil)
	//err := DB().Table(tht(id)).Select("id", "host", "status").Where("id=?", id).Order("ii").Find(&ret).Error
	return ret, err
}

func TaskHostGets(id int64) ([]TaskHost, error) {
	ret := make([]TaskHost, 0)
	finder := zorm.NewSelectFinder(tht(id)).Append("WHERE id=? order by ii asc", id)
	err := zorm.Query(context.Background(), finder, &ret, nil)
	//err := DB().Table(tht(id)).Where("id=?", id).Order("ii").Find(&ret).Error
	return ret, err
}

var (
	taskHostCache = make([]TaskHost, 0, 128)
	taskHostLock  sync.RWMutex
)

func TaskHostCachePush(taskHost TaskHost) {
	taskHostLock.Lock()
	defer taskHostLock.Unlock()

	taskHostCache = append(taskHostCache, taskHost)
}

func TaskHostCachePopAll() []TaskHost {
	taskHostLock.Lock()
	defer taskHostLock.Unlock()

	all := taskHostCache
	taskHostCache = make([]TaskHost, 0, 128)

	return all
}

func ReportCacheResult() error {
	result := TaskHostCachePopAll()
	reports := make([]TaskHost, 0)
	for _, th := range result {
		// id大于redis初始id，说明是edge与center失联时，本地告警规则触发的自愈脚本生成的id
		// 为了防止不同边缘机房生成的脚本任务id相同，不上报结果至数据库
		if th.Id >= storage.IDINITIAL {
			logger.Infof("task[%d] host[%s] done, result:[%v]", th.Id, th.Host, th)
		} else {
			reports = append(reports, th)
		}
	}

	if len(reports) == 0 {
		return nil
	}

	errs, err := TaskHostUpserts(reports)
	if err != nil {
		return err
	}
	for key, err := range errs {
		logger.Warningf("report task_host_cache[%s] result error: %v", key, err)
	}
	return nil
}
