package models

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/didi/nightingale/v5/src/pkg/ormx"
	"github.com/pkg/errors"
)

//AlertSubscribeStructTableName 表名常量,方便直接调用
const AlertSubscribeStructTableName = "alert_subscribe"

// AlertSubscribeStruct
type AlertSubscribe struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id               int64        `column:"id" json:"id"`
	GroupId          int64        `column:"group_id" json:"group_id"` //GroupId busi group id
	Cluster          string       `column:"cluster" json:"cluster"`
	RuleId           int64        `column:"rule_id" json:"rule_id"`
	Tags             ormx.JSONArr `column:"tags" json:"tags"`
	RedefineSeverity int          `column:"redefine_severity" json:"redefine_severity"` //RedefineSeverity is redefine severity?
	NewSeverity      int          `column:"new_severity" json:"new_severity"`           //NewSeverity 0:Emergency 1:Warning 2:Notice
	RedefineChannels int          `column:"redefine_channels" json:"redefine_channels"` //RedefineChannels is redefine channels?
	NewChannels      string       `column:"new_channels" json:"new_channels"`           //NewChannels split by space: sms voice email dingtalk wecom
	UserGroupIds     string       `column:"user_group_ids" json:"user_group_ids"`       //UserGroupIds split by space 1 34 5, notify cc to user_group_ids
	CreateAt         int64        `column:"create_at" json:"create_at"`
	CreateBy         string       `column:"create_by" json:"create_by"`
	UpdateAt         int64        `column:"update_at" json:"update_at"`
	UpdateBy         string       `column:"update_by" json:"update_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	UserGroups []UserGroup `json:"user_groups"` // for fe
	RuleName   string      `json:"rule_name"`   // for fe
	ITags      []TagFilter `json:"-"`           // inner tags
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertSubscribe) GetTableName() string {
	return AlertSubscribeStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertSubscribe) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func AlertSubscribeGets(groupId int64) (lst []AlertSubscribe, err error) {
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertSubscribeStructTableName) // select * from t_demo
	finder.Append(" Where group_id=?", groupId).Append(" Order by id desc")
	err = zorm.Query(ctx, finder, &lst, nil)
	// err = DB().Where("group_id=?", groupId).Order("id desc").Find(&lst).Error
	return
}

func AlertSubscribeGet(where string, args ...interface{}) (*AlertSubscribe, error) {
	lst := make([]*AlertSubscribe, 0)
	// err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertSubscribeStructTableName) // select * from t_demo
	if where != "" {
		finder.Append("Where "+where, args...)
	}

	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}

func (s *AlertSubscribe) Verify() error {
	if s.Cluster == "" {
		return errors.New("cluster invalid")
	}

	if IsClusterAll(s.Cluster) {
		s.Cluster = ClusterAll
	}

	if err := s.Parse(); err != nil {
		return err
	}

	if len(s.ITags) == 0 && s.RuleId == 0 {
		return errors.New("rule_id and tags are both blank")
	}

	ugids := strings.Fields(s.UserGroupIds)
	for i := 0; i < len(ugids); i++ {
		if _, err := strconv.ParseInt(ugids[i], 10, 64); err != nil {
			return errors.New("user_group_ids invalid")
		}
	}

	return nil
}

func (s *AlertSubscribe) Parse() error {
	err := json.Unmarshal(s.Tags, &s.ITags)
	if err != nil {
		return err
	}

	for i := 0; i < len(s.ITags); i++ {
		if s.ITags[i].Func == "=~" {
			s.ITags[i].Regexp, err = regexp.Compile(s.ITags[i].Value)
			if err != nil {
				return err
			}
		} else if s.ITags[i].Func == "in" {
			arr := strings.Fields(s.ITags[i].Value)
			s.ITags[i].Vset = make(map[string]struct{})
			for j := 0; j < len(arr); j++ {
				s.ITags[i].Vset[arr[j]] = struct{}{}
			}
		}
	}

	return nil
}

func (s *AlertSubscribe) Add() error {
	if err := s.Verify(); err != nil {
		return err
	}

	now := time.Now().Unix()
	s.CreateAt = now
	s.UpdateAt = now
	return Insert(s)
}

func (s *AlertSubscribe) FillRuleName(cache map[int64]string) error {
	if s.RuleId <= 0 {
		s.RuleName = ""
		return nil
	}

	name, has := cache[s.RuleId]
	if has {
		s.RuleName = name
		return nil
	}

	name, err := AlertRuleGetName(s.RuleId)
	if err != nil {
		return err
	}

	if name == "" {
		name = "Error: AlertRule not found"
	}

	s.RuleName = name
	cache[s.RuleId] = name
	return nil
}

func (s *AlertSubscribe) FillUserGroups(cache map[int64]*UserGroup) error {
	// some user-group already deleted ?
	ugids := strings.Fields(s.UserGroupIds)

	count := len(ugids)
	if count == 0 {
		s.UserGroups = []UserGroup{}
		return nil
	}

	exists := make([]string, 0, count)
	delete := false
	for i := range ugids {
		id, _ := strconv.ParseInt(ugids[i], 10, 64)

		ug, has := cache[id]
		if has {
			exists = append(exists, ugids[i])
			s.UserGroups = append(s.UserGroups, *ug)
			continue
		}

		ug, err := UserGroupGetById(id)
		if err != nil {
			return err
		}

		if ug == nil {
			delete = true
		} else {
			exists = append(exists, ugids[i])
			s.UserGroups = append(s.UserGroups, *ug)
			cache[id] = ug
		}
	}

	if delete {
		// some user-group already deleted
		// DB().Model(s).Update("user_group_ids", strings.Join(exists, " "))
		ctx := getCtx()
		s.UserGroupIds = strings.Join(exists, " ")
		finder := zorm.NewUpdateFinder(AlertSubscribeStructTableName)
		finder.Append("user_group_ids=? WHERE id=?", s.UserGroupIds, s.Id)
		_, _ = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			_, err := zorm.UpdateFinder(ctx, finder)
			//如果返回的err不是nil,事务就会回滚
			return nil, err
		})
		//s.UserGroupIds = strings.Join(exists, " ")
	}

	return nil
}

func (s *AlertSubscribe) Update(selectField interface{}, selectFields ...interface{}) error {
	if err := s.Verify(); err != nil {
		return err
	}

	// return DB().Model(s).Select(selectField, selectFields...).Updates(s).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, s)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func AlertSubscribeDel(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(AlertSubscribeStructTableName)
		finder.Append("Where id in (?)", ids)
		_, err := zorm.UpdateFinder(ctx, finder)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err

	// return DB().Where("id in ?", ids).Delete(new(AlertSubscribe)).Error
}

func AlertSubscribeStatistics(cluster string) (*Statistics, error) {
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertRuleStructTableName, "count(*) as total, max(update_at) as last_updated")
	if cluster != "" {
		// session = session.Where("(cluster like ? or cluster = ?)", "%"+cluster+"%", ClusterAll)
		finder.Append(" Where (cluster like ? or cluster = ?)", "%"+cluster+"%", ClusterAll)
	}
	stats := make([]*Statistics, 0)
	// err := session.Find(&stats).Error
	err := zorm.Query(ctx, finder, &stats, nil)
	if err != nil {
		return nil, err
	}

	return stats[0], nil
}

func AlertSubscribeGetsByCluster(cluster string) ([]*AlertSubscribe, error) {
	lst := make([]*AlertSubscribe, 0)
	slst := make([]*AlertSubscribe, 0)
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertSubscribeStructTableName) // select * from t_demo
	if cluster != "" {
		// session = session.Where("(cluster like ? or cluster = ?)", "%"+cluster+"%", ClusterAll)
		finder.Append("Where(cluster like ? or cluster = ?)", "%"+cluster+"%", ClusterAll)
	}

	err := zorm.Query(ctx, finder, &lst, nil)

	// get my cluster's subscribes
	// session := DB().Model(&AlertSubscribe{})
	// if cluster != "" {
	// 	session = session.Where("cluster = ?", cluster)
	// }

	// err := session.Find(&lst).Error
	if err != nil {
		return nil, err
	}
	for _, s := range lst {
		if MatchCluster(s.Cluster, cluster) {
			slst = append(slst, s)
		}
	}
	return slst, err
}
