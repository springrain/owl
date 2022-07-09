package models

import (
	"strconv"
	"strings"

	"gitee.com/chunanyong/zorm"
)

//AlertHisEventStructTableName 表名常量,方便直接调用
const AlertHisEventStructTableName = "alert_his_event"

// AlertHisEvent
type AlertHisEvent struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	//Id []
	Id               int64  `column:"id" json:"id"`
	IsRecovered      int    `column:"is_recovered" json:"is_recovered"`
	Cluster          string `column:"cluster" json:"cluster"`
	GroupId          int64  `column:"group_id" json:"group_id"`
	GroupName        string `column:"group_name" json:"group_name"`
	Hash             string `column:"hash" json:"hash"`
	RuleId           int64  `column:"rule_id" json:"rule_id"`
	RuleName         string `column:"rule_name" json:"rule_name"`
	RuleNote         string `column:"rule_note" json:"rule_note"`
	RuleProd         string `column:"rule_prod" json:"rule_prod"`
	RuleAlgo         string `column:"rule_algo" json:"rule_algo"`
	Severity         int    `column:"severity" json:"severity"`
	PromForDuration  int    `column:"prom_for_duration" json:"prom_for_duration"`
	PromQl           string `column:"prom_ql" json:"prom_ql"`
	PromEvalInterval int    `column:"prom_eval_interval" json:"prom_eval_interval"`
	Callbacks        string `column:"callbacks" json:"-"`
	RunbookUrl       string `column:"runbook_url" json:"runbook_url"`
	NotifyRecovered  int    `column:"notify_recovered" json:"notify_recovered"`
	NotifyChannels   string `column:"notify_channels" json:"-"`
	NotifyGroups     string `column:"notify_groups" json:"-"`
	TargetIdent      string `column:"target_ident" json:"target_ident"`
	TargetNote       string `column:"target_note" json:"target_note"`
	TriggerTime      int64  `column:"trigger_time" json:"trigger_time"`
	TriggerValue     string `column:"trigger_value" json:"trigger_value"`
	RecoverTime      int64  `column:"recover_time" json:"recover_time"`
	LastEvalTime     int64  `column:"last_eval_time" json:"last_eval_time"`
	Tags             string `column:"tags" json:"-"`
	NotifyCurNumber  int    `column:"notify_cur_number" json:"notify_cur_number"` // notify: current number

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	CallbacksJSON      []string    `json:"callbacks"`
	NotifyChannelsJSON []string    `json:"notify_channels"`
	NotifyGroupsJSON   []string    `json:"notify_groups"`
	NotifyGroupsObj    []UserGroup `json:"notify_groups_obj"`
	TagsJSON           []string    `json:"tags"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertHisEvent) GetTableName() string {
	return AlertHisEventStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertHisEvent) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (e *AlertHisEvent) Add() error {
	return Insert(e)
}

func (e *AlertHisEvent) DB2FE() {
	e.NotifyChannelsJSON = strings.Fields(e.NotifyChannels)
	e.NotifyGroupsJSON = strings.Fields(e.NotifyGroups)
	e.CallbacksJSON = strings.Fields(e.Callbacks)
	e.TagsJSON = strings.Split(e.Tags, ",,")
}

func (e *AlertHisEvent) FillNotifyGroups(cache map[int64]*UserGroup) error {
	// some user-group already deleted ?
	count := len(e.NotifyGroupsJSON)
	if count == 0 {
		e.NotifyGroupsObj = []UserGroup{}
		return nil
	}

	for i := range e.NotifyGroupsJSON {
		id, err := strconv.ParseInt(e.NotifyGroupsJSON[i], 10, 64)
		if err != nil {
			continue
		}

		ug, has := cache[id]
		if has {
			e.NotifyGroupsObj = append(e.NotifyGroupsObj, *ug)
			continue
		}

		ug, err = UserGroupGetById(id)
		if err != nil {
			return err
		}

		if ug != nil {
			e.NotifyGroupsObj = append(e.NotifyGroupsObj, *ug)
			cache[id] = ug
		}
	}

	return nil
}

func AlertHisEventTotal(prod string, bgid, stime, etime int64, severity int, recovered int, clusters []string, query string) (int64, error) {
	// session := DB().Model(&AlertHisEvent{}).Where("last_eval_time between ? and ? and rule_prod = ?", stime, etime, prod)

	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertHisEventStructTableName, "count(*)")
	finder.Append("Where last_eval_time between ? and ? and rule_prod = ?", stime, etime, prod)
	if bgid > 0 {
		finder.Append(" and group_id = ?", bgid)
	}
	if severity >= 0 {
		// session = session.Where("severity = ?", severity)
		finder.Append(" And severity = ?", severity)
	}

	if recovered >= 0 {
		// session = session.Where("is_recovered = ?", recovered)
		finder.Append(" And is_recovered = ?", recovered)
	}

	if len(clusters) > 0 {
		// session = session.Where("cluster in ?", clusters)
		finder.Append(" And cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			// session = session.Where("rule_name like ? or tags like ?", qarg, qarg)
			finder.Append(" And (rule_name like ? or tags like ?) ", qarg, qarg)
		}
	}

	return Count(finder)
}

func AlertHisEventGets(prod string, bgid, stime, etime int64, severity int, recovered int, clusters []string, query string, limit, offset int) ([]AlertHisEvent, error) {

	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertHisEventStructTableName) // select * from t_demo
	page := zorm.NewPage()
	page.PageNo = offset/limit + 1 //查询第1页,默认是1
	page.PageSize = limit
	finder.Append("Where last_eval_time between ? and ? and rule_prod = ?", stime, etime, prod)
	if bgid > 0 {
		finder.Append(" and group_id = ?", bgid)
	}

	if severity >= 0 {
		// session = session.Where("severity = ?", severity)
		finder.Append(" And severity = ?", severity)
	}

	if recovered >= 0 {
		// session = session.Where("is_recovered = ?", recovered)
		finder.Append(" And is_recovered = ?", recovered)
	}

	if len(clusters) > 0 {
		// session = session.Where("cluster in ?", clusters)
		finder.Append(" And cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			finder.Append(" And (rule_name like ? or tags like ?) ", qarg, qarg)
		}
	}

	lst := make([]AlertHisEvent, 0)
	// err := session.Order("id desc").Limit(limit).Offset(offset).Find(&lst).Error
	finder.Append(" Order by id desc ")
	err := zorm.Query(ctx, finder, &lst, page)

	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}

	return lst, err
}

func AlertHisEventGet(where string, args ...interface{}) (*AlertHisEvent, error) {
	lst := make([]*AlertHisEvent, 0)
	// err := DB().Where(where, args...).Find(&lst).Error

	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertHisEventStructTableName) // select * from t_demo
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

	lst[0].DB2FE()
	lst[0].FillNotifyGroups(make(map[int64]*UserGroup))

	return lst[0], nil
}

func AlertHisEventGetById(id int64) (*AlertHisEvent, error) {
	return AlertHisEventGet("id=?", id)
}
