package models

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/didi/nightingale/v5/src/pkg/ormx"

	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
)

type TagFilter struct {
	Key    string              `json:"key"`   // tag key
	Func   string              `json:"func"`  // == | =~ | in
	Value  string              `json:"value"` // tag value
	Regexp *regexp.Regexp      // parse value to regexp if func = '=~'
	Vset   map[string]struct{} // parse value to regexp if func = 'in'
}

//AlertMuteStructTableName 表名常量,方便直接调用
const AlertMuteStructTableName = "alert_mute"

// AlertMute
type AlertMute struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id       int64        `column:"id" json:"id"`
	GroupId  int64        `column:"group_id" json:"group_id"`
	Prod     string       `column:"prod" json:"prod"` // product empty means n9e
	Cluster  string       `column:"cluster" json:"cluster"`
	Tags     ormx.JSONArr `column:"tags" json:"tags"`
	Cause    string       `column:"cause" json:"cause"`
	Btime    int64        `column:"btime" json:"btime"`
	Etime    int64        `column:"etime" json:"etime"`
	CreateAt int64        `column:"create_at" json:"create_at"`
	CreateBy string       `column:"create_by" json:"create_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	ITags []TagFilter `json:"-"` // inner tags
}

func (entity *AlertMute) GetTableName() string {
	return AlertMuteStructTableName
}

func (entity *AlertMute) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func AlertMuteGets(prods []string, bgid int64, query string) (lst []AlertMute, err error) {
	// session := DB().Where("group_id = ? and prod in (?)", bgid, prods)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertMuteStructTableName)
	finder.Append("Where group_id = ? and prod in (?)", bgid, prods)

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			// session = session.Where("cause like ?", qarg, qarg)
			finder.Append(" And cause like ?", qarg)
		}
	}

	finder.Append(" Order by id desc")

	err = zorm.Query(ctx, finder, &lst, nil)
	return
}

func AlertMuteGetsByBG(groupId int64) (lst []AlertMute, err error) {
	// err = DB().Where("group_id=?", groupId).Order("id desc").Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertMuteStructTableName) // select * from t_demo
	finder.Append("Where group_id=?", groupId)
	finder.Append(" Order by id desc")
	err = zorm.Query(ctx, finder, &lst, nil)
	return
}

func (m *AlertMute) Verify() error {
	if m.GroupId < 0 {
		return errors.New("group_id invalid")
	}

	if m.Cluster == "" {
		return errors.New("cluster invalid")
	}

	if m.Etime <= m.Btime {
		return fmt.Errorf("Oops... etime(%d) <= btime(%d)", m.Etime, m.Btime)
	}

	if err := m.Parse(); err != nil {
		return err
	}

	if len(m.ITags) == 0 {
		return errors.New("tags is blank")
	}

	return nil
}

func (m *AlertMute) Parse() error {
	err := json.Unmarshal(m.Tags, &m.ITags)
	if err != nil {
		return err
	}

	for i := 0; i < len(m.ITags); i++ {
		if m.ITags[i].Func == "=~" || m.ITags[i].Func == "!~" {
			m.ITags[i].Regexp, err = regexp.Compile(m.ITags[i].Value)
			if err != nil {
				return err
			}
		} else if m.ITags[i].Func == "in" || m.ITags[i].Func == "not in" {
			arr := strings.Fields(m.ITags[i].Value)
			m.ITags[i].Vset = make(map[string]struct{})
			for j := 0; j < len(arr); j++ {
				m.ITags[i].Vset[arr[j]] = struct{}{}
			}
		}
	}

	return nil
}

func (m *AlertMute) Add() error {
	if err := m.Verify(); err != nil {
		return err
	}
	m.CreateAt = time.Now().Unix()
	return Insert(m)
}

func AlertMuteDel(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	// return DB().Where("id in ?", ids).Delete(new(AlertMute)).Error

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(AlertMuteStructTableName)
		finder.Append(" Where id in (?)", ids)
		_, err := zorm.UpdateFinder(ctx, finder)

		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func AlertMuteStatistics(cluster string) (*Statistics, error) {
	stats := make([]*Statistics, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertMuteStructTableName, "count(*) as total, max(create_at) as last_updated")
	if cluster != "" {
		// session = session.Where("cluster = ?", cluster)
		finder.Append(" Where cluster = ?", cluster)
	}

	err := zorm.Query(ctx, finder, &stats, nil)

	if err != nil {
		return nil, err
	}
	return stats[0], nil
}

func AlertMuteGetsByCluster(cluster string) ([]*AlertMute, error) {
	// clean expired first
	buf := int64(30)
	// err := DB().Where("etime < ?", time.Now().Unix()+buf).Delete(new(AlertMute)).Error

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(AlertMuteStructTableName)
		finder.Append(" Where etime < ?", time.Now().Unix()+buf)
		_, err := zorm.UpdateFinder(ctx, finder)

		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})

	if err != nil {
		return nil, err
	}

	// get my cluster's mutes

	finder := zorm.NewSelectFinder(AlertMuteStructTableName) // select * from t_demo

	if cluster != "" {
		// session = session.Where("cluster = ?", cluster)
		finder.Append(" Where cluster = ?", cluster)
	}

	// session := DB().Model(&AlertMute{})

	lst := make([]*AlertMute, 0)
	// err = session.Find(&lst).Error
	err = zorm.Query(ctx, finder, &lst, nil)

	return lst, err
}
