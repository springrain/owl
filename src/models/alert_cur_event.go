package models

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"gitee.com/chunanyong/zorm"
	"github.com/didi/nightingale/v5/src/pkg/tplx"
)

const AlertCurEventStructTableName = "alert_cur_event"

// AlertCurEventStruct
type AlertCurEvent struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id               int64  `column:"id" json:"id"`
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
	Tags             string `column:"tags" json:"-"`
	NotifyCurNumber  int    `column:"notify_cur_number" json:"notify_cur_number"` // notify: current number
	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	CallbacksJSON      []string          `json:"callbacks"`         // for fe
	NotifyChannelsJSON []string          `json:"notify_channels"`   // for fe
	NotifyGroupsJSON   []string          `json:"notify_groups"`     // for fe
	NotifyGroupsObj    []*UserGroup      `json:"notify_groups_obj"` // for fe
	TagsJSON           []string          `json:"tags"`              // for fe
	TagsMap            map[string]string `json:"-"`                 // for internal usage
	IsRecovered        bool              `json:"is_recovered"`      // for notify.py
	NotifyUsersObj     []*User           `json:"notify_users_obj"`  // for notify.py
	LastEvalTime       int64             `json:"last_eval_time"`    // for notify.py 上次计算的时间
	LastSentTime       int64             `json:"last_sent_time"`    // 上次发送时间
}

func (entity *AlertCurEvent) GetTableName() string {
	return AlertCurEventStructTableName
}

func (entity *AlertCurEvent) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (e *AlertCurEvent) TableName() string {
	return "alert_cur_event"
}

func (e *AlertCurEvent) Add() error {
	// return Insert(e)
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {

		_, err := zorm.Insert(ctx, e)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

type AggrRule struct {
	Type  string
	Value string
}

func (e *AlertCurEvent) ParseRuleNote() error {
	e.RuleNote = strings.TrimSpace(e.RuleNote)

	if e.RuleNote == "" {
		return nil
	}

	var defs = []string{
		"{{$labels := .TagsMap}}",
		"{{$value := .TriggerValue}}",
	}

	text := strings.Join(append(defs, e.RuleNote), "")
	t, err := template.New(fmt.Sprint(e.RuleId)).Funcs(tplx.TemplateFuncMap).Parse(text)
	if err != nil {
		return err
	}

	var body bytes.Buffer
	err = t.Execute(&body, e)
	if err != nil {
		return err
	}

	e.RuleNote = body.String()
	return nil
}

func (e *AlertCurEvent) GenCardTitle(rules []*AggrRule) string {
	arr := make([]string, len(rules))
	for i := 0; i < len(rules); i++ {
		rule := rules[i]

		if rule.Type == "field" {
			arr[i] = e.GetField(rule.Value)
		}

		if rule.Type == "tagkey" {
			arr[i] = e.GetTagValue(rule.Value)
		}

		if len(arr[i]) == 0 {
			arr[i] = "Null"
		}
	}
	return strings.Join(arr, "::")
}

func (e *AlertCurEvent) GetTagValue(tagkey string) string {
	for _, tag := range e.TagsJSON {
		i := strings.Index(tag, tagkey+"=")
		if i >= 0 {
			return tag[len(tagkey+"="):]
		}
	}
	return ""
}

func (e *AlertCurEvent) GetField(field string) string {
	switch field {
	case "cluster":
		return e.Cluster
	case "group_id":
		return fmt.Sprint(e.GroupId)
	case "group_name":
		return e.GroupName
	case "rule_id":
		return fmt.Sprint(e.RuleId)
	case "rule_name":
		return e.RuleName
	case "severity":
		return fmt.Sprint(e.Severity)
	case "runbook_url":
		return e.RunbookUrl
	case "target_ident":
		return e.TargetIdent
	case "target_note":
		return e.TargetNote
	default:
		return ""
	}
}

func (e *AlertCurEvent) ToHis() *AlertHisEvent {
	isRecovered := 0
	var recoverTime int64 = 0
	if e.IsRecovered {
		isRecovered = 1
		recoverTime = e.LastEvalTime
	}

	return &AlertHisEvent{
		IsRecovered:      isRecovered,
		Cluster:          e.Cluster,
		GroupId:          e.GroupId,
		GroupName:        e.GroupName,
		Hash:             e.Hash,
		RuleId:           e.RuleId,
		RuleName:         e.RuleName,
		RuleProd:         e.RuleProd,
		RuleAlgo:         e.RuleAlgo,
		RuleNote:         e.RuleNote,
		Severity:         e.Severity,
		PromForDuration:  e.PromForDuration,
		PromQl:           e.PromQl,
		PromEvalInterval: e.PromEvalInterval,
		Callbacks:        e.Callbacks,
		RunbookUrl:       e.RunbookUrl,
		NotifyRecovered:  e.NotifyRecovered,
		NotifyChannels:   e.NotifyChannels,
		NotifyGroups:     e.NotifyGroups,
		TargetIdent:      e.TargetIdent,
		TargetNote:       e.TargetNote,
		TriggerTime:      e.TriggerTime,
		TriggerValue:     e.TriggerValue,
		Tags:             e.Tags,
		RecoverTime:      recoverTime,
		LastEvalTime:     e.LastEvalTime,
		NotifyCurNumber:  e.NotifyCurNumber,
	}
}

func (e *AlertCurEvent) DB2FE() {
	e.NotifyChannelsJSON = strings.Fields(e.NotifyChannels)
	e.NotifyGroupsJSON = strings.Fields(e.NotifyGroups)
	e.CallbacksJSON = strings.Fields(e.Callbacks)
	e.TagsJSON = strings.Split(e.Tags, ",,")
}

func (e *AlertCurEvent) DB2Mem() {
	e.IsRecovered = false
	e.NotifyGroupsJSON = strings.Fields(e.NotifyGroups)
	e.CallbacksJSON = strings.Fields(e.Callbacks)
	e.NotifyChannelsJSON = strings.Fields(e.NotifyChannels)
	e.TagsJSON = strings.Split(e.Tags, ",,")
	e.TagsMap = make(map[string]string)
	for i := 0; i < len(e.TagsJSON); i++ {
		pair := strings.TrimSpace(e.TagsJSON[i])
		if pair == "" {
			continue
		}

		arr := strings.Split(pair, "=")
		if len(arr) != 2 {
			continue
		}

		e.TagsMap[arr[0]] = arr[1]
	}
}

// for webui
func (e *AlertCurEvent) FillNotifyGroups(cache map[int64]*UserGroup) error {
	// some user-group already deleted ?
	count := len(e.NotifyGroupsJSON)
	if count == 0 {
		e.NotifyGroupsObj = []*UserGroup{}
		return nil
	}

	for i := range e.NotifyGroupsJSON {
		id, err := strconv.ParseInt(e.NotifyGroupsJSON[i], 10, 64)
		if err != nil {
			continue
		}

		ug, has := cache[id]
		if has {
			e.NotifyGroupsObj = append(e.NotifyGroupsObj, ug)
			continue
		}

		ug, err = UserGroupGetById(id)
		if err != nil {
			return err
		}

		if ug != nil {
			e.NotifyGroupsObj = append(e.NotifyGroupsObj, ug)
			cache[id] = ug
		}
	}

	return nil
}

func AlertCurEventTotal(prod string, bgid, stime, etime int64, severity int, clusters []string, query string) (int64, error) {

	// session := DB().Model(&AlertCurEvent{}).Where("trigger_time between ? and ? and rule_prod = ?", stime, etime, prod)
	// return Count(session)

	finder := zorm.NewSelectFinder(AlertCurEventStructTableName, "count(*)")
	finder.Append("Where trigger_time between ? and ? and rule_prod = ?", stime, etime, prod)

	if bgid > 0 {
		finder.Append("And group_id = ?", bgid)
		// session = session.Where("group_id = ?", bgid)
	}

	if severity >= 0 {
		// session = session.Where("severity = ?", severity)
		finder.Append("And severity = ?", severity)
	}

	if len(clusters) > 0 {
		// session = session.Where("cluster in ?", clusters)
		finder.Append("And cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			// session = session.Where("rule_name like ? or tags like ?", qarg, qarg)
			finder.Append(" And (rule_name like ? or tags like ?) ", qarg, qarg)
		}
	}
	//执行查询
	num, err := Count(finder)
	if err != nil {
		return 0, err
	}
	return num, err

}

func AlertCurEventGets(prod string, bgid, stime, etime int64, severity int, clusters []string, query string, limit, offset int) ([]AlertCurEvent, error) {
	// session := DB().Where("trigger_time between ? and ? and rule_prod = ?", stime, etime, prod)

	lst := make([]AlertCurEvent, 0)
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName) // select * from t_demo
	finder.Append("Where trigger_time between ? and ? and rule_prod = ?", stime, etime, prod)

	if bgid > 0 {
		finder.Append(" and group_id = ?", bgid)
	}

	if severity >= 0 {
		finder.Append("And severity = ?", severity)
	}

	if len(clusters) > 0 {
		finder.Append("And cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			// session = session.Where("rule_name like ? or tags like ?", qarg, qarg)
			finder.Append(" And (rule_name like ? or tags like ?)", qarg, qarg)
		}
	}

	page := zorm.NewPage()
	page.PageNo = offset/limit + 1 //查询第1页,默认是1
	page.PageSize = limit
	finder.Append(" Order by id desc")
	err := zorm.Query(ctx, finder, &lst, page)
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}
	return lst, err
}

func AlertCurEventDel(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	// return DB().Where("id in ?", ids).Delete(&AlertCurEvent{}).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(AlertCurEventStructTableName)
		finder.Append("Where id in (?)", ids)
		_, err := zorm.UpdateFinder(ctx, finder)

		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func AlertCurEventDelByHash(hash string) error {
	// return DB().Where("hash = ?", hash).Delete(&AlertCurEvent{}).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(AlertCurEventStructTableName)
		finder.Append("Where hash = ?", hash)
		_, err := zorm.UpdateFinder(ctx, finder)

		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func AlertCurEventExists(where string, args ...interface{}) (bool, error) {
	// return Exists(DB().Model(&AlertCurEvent{}).Where(where, args...))
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName, "count(*)")
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	//查询条数
	num, err := Count(finder)
	return num > 0, err

}

func AlertCurEventGet(where string, args ...interface{}) (*AlertCurEvent, error) {
	lst := make([]*AlertCurEvent, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName) // select * from t_demo

	if where != "" {
		finder.Append("Where "+where, args...)
	}
	//执行查询
	err := zorm.Query(ctx, finder, &lst, nil)
	if len(lst) == 0 {
		return nil, err
	}

	lst[0].DB2FE()
	lst[0].FillNotifyGroups(make(map[int64]*UserGroup))

	return lst[0], nil
}

func AlertCurEventGetById(id int64) (*AlertCurEvent, error) {
	return AlertCurEventGet("id=?", id)
}

type AlertNumber struct {
	GroupId    int64
	GroupCount int64
}

// for busi_group list page
func AlertNumbers(bgids []int64) (map[int64]int64, error) {
	ret := make(map[int64]int64)
	if len(bgids) == 0 {
		return ret, nil
	}

	arr := make([]AlertNumber, 0)
	// err := DB().Model(&AlertCurEvent{}).Select("group_id", "count(*) as group_count").Where("group_id in ?", bgids).Group("group_id").Find(&arr).Error

	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName, "count(*) as group_count, group_id")
	finder.Append(" Where group_id in (?)", bgids).Append(" Group by group_id")
	err := zorm.Query(ctx, finder, &arr, nil)

	if err != nil {
		return nil, err
	}

	for i := 0; i < len(arr); i++ {
		ret[arr[i].GroupId] = arr[i].GroupCount
	}

	return ret, nil
}

func AlertCurEventGetAll(cluster string) ([]*AlertCurEvent, error) {
	// session := DB().Model(&AlertCurEvent{})
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName) // select * from t_demo

	if cluster != "" {
		// session = session.Where("cluster = ?", cluster)
		finder.Append("Where cluster = ?", cluster)
	}

	lst := make([]*AlertCurEvent, 0)
	// err := session.Find(&lst).Error

	//执行查询
	err := zorm.Query(ctx, finder, &lst, nil)

	return lst, err
}

func AlertCurEventGetByIds(ids []int64) ([]*AlertCurEvent, error) {
	lst := make([]*AlertCurEvent, 0)

	if len(ids) == 0 {
		return lst, nil
	}

	// err := DB().Where("id in ?", ids).Order("id desc").Find(&lst).Error

	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName)
	finder.Append(" Where id in (?) ", ids)
	finder.Append("order by id desc ")
	err := zorm.Query(ctx, finder, &lst, nil)

	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}

	return lst, err
}

func AlertCurEventGetByRule(ruleId int64) ([]*AlertCurEvent, error) {
	lst := make([]*AlertCurEvent, 0)
	// err := DB().Where("rule_id=?", ruleId).Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName)
	finder.Append("Where rule_id=?", ruleId)
	err := zorm.Query(ctx, finder, &lst, nil)
	return lst, err
}

func AlertCurEventGetMap(cluster string) (map[int64]map[string]struct{}, error) {
	// session := DB().Model(&AlertCurEvent{})
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertCurEventStructTableName, "rule_id,hash")
	if cluster != "" {
		// session = session.Where("cluster = ?", cluster)
		finder.Append("Where cluster = ?", cluster)
	}

	lst := make([]*AlertCurEvent, 0)
	// err := session.Select("rule_id", "hash").Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}

	ret := make(map[int64]map[string]struct{})
	for i := 0; i < len(lst); i++ {
		rid := lst[i].RuleId
		hash := lst[i].Hash
		if _, has := ret[rid]; has {
			ret[rid][hash] = struct{}{}
		} else {
			ret[rid] = make(map[string]struct{})
			ret[rid][hash] = struct{}{}
		}
	}

	return ret, nil
}
