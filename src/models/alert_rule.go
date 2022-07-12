package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/didi/nightingale/v5/src/webapi/config"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/str"
)

//AlertRuleStructTableName 表名常量,方便直接调用
const AlertRuleStructTableName = "alert_rule"

// AlertRuleStruct
type AlertRule struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id               int64  `column:"id" json:"id"`
	GroupId          int64  `column:"group_id" json:"group_id"` //GroupId busi group id
	Cluster          string `column:"cluster" json:"cluster"`
	Name             string `column:"name" json:"name"`
	Note             string `column:"note" json:"note"`
	Prod             string `column:"prod" json:"prod"`                             // product empty means n9e
	Algorithm        string `column:"algorithm" json:"algorithm"`                   // algorithm (''|holtwinters), empty means threshold
	AlgoParams       string `column:"algo_params" json:"-"`                         // params algorithm need
	Delay            int    `column:"delay" json:"delay"`                           // Time (in seconds) to delay evaluation
	Severity         int    `column:"severity" json:"severity"`                     //Severity 0:Emergency 1:Warning 2:Notice
	Disabled         int    `column:"disabled" json:"disabled"`                     //Disabled 0:enabled 1:disabled
	PromForDuration  int    `column:"prom_for_duration" json:"prom_for_duration"`   //PromForDuration prometheus for, unit:s
	PromQl           string `column:"prom_ql" json:"prom_ql"`                       //PromQl promql
	PromEvalInterval int    `column:"prom_eval_interval" json:"prom_eval_interval"` //PromEvalInterval evaluate interval
	EnableStime      string `column:"enable_stime" json:"enable_stime"`             //EnableStime []
	EnableEtime      string `column:"enable_etime" json:"enable_etime"`             //EnableEtime []
	EnableDaysOfWeek string `column:"enable_days_of_week" json:"-"`                 //EnableDaysOfWeek split by space: 0 1 2 3 4 5 6
	NotifyRecovered  int    `column:"notify_recovered" json:"notify_recovered"`     //NotifyRecovered whether notify when recovery
	NotifyChannels   string `column:"notify_channels" json:"-"`                     //NotifyChannels split by space: sms voice email dingtalk wecom
	NotifyGroups     string `column:"notify_groups" json:"-"`                       //NotifyGroups split by space: 233 43
	NotifyRepeatStep int    `column:"notify_repeat_step" json:"notify_repeat_step"` //NotifyRepeatStep unit: min
	NotifyMaxNumber  int    `column:"notify_max_number" json:"notify_max_number"`   // notify: max number
	Callbacks        string `column:"callbacks" json:"-"`                           //Callbacks split by space: http://a.com/api/x http://a.com/api/y
	RunbookUrl       string `column:"runbook_url" json:"runbook_url"`               //RunbookUrl []
	AppendTags       string `column:"append_tags" json:"-"`                         //AppendTags split by space: service=n9e mod=api
	CreateAt         int64  `column:"create_at" json:"create_at"`
	CreateBy         string `column:"create_by" json:"create_by"`
	UpdateAt         int64  `column:"update_at" json:"update_at"`
	UpdateBy         string `column:"update_by" json:"update_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	EnableDaysOfWeekJSON []string    `json:"enable_days_of_week"` // for fe
	EnableInBG           int         `json:"enable_in_bg"`        // 0: global 1: enable one busi-group
	NotifyChannelsJSON   []string    `json:"notify_channels"`     // for fe
	NotifyGroupsObj      []UserGroup `json:"notify_groups_obj"`   // for fe
	NotifyGroupsJSON     []string    `json:"notify_groups"`       // for fe
	RecoverDuration      int64       `json:"recover_duration"`    // unit: s
	CallbacksJSON        []string    `json:"callbacks"`           // for fe
	AppendTagsJSON       []string    `json:"append_tags"`         // for fe
	AlgoParamsJson       interface{} `json:"algo_params"`         //
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertRule) GetTableName() string {
	return AlertRuleStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertRule) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (ar *AlertRule) Verify() error {
	if ar.GroupId < 0 {
		return fmt.Errorf("GroupId(%d) invalid", ar.GroupId)
	}

	if ar.Cluster == "" {
		return errors.New("cluster is blank")
	}

	if str.Dangerous(ar.Name) {
		return errors.New("Name has invalid characters")
	}

	if ar.Name == "" {
		return errors.New("name is blank")
	}

	if ar.PromQl == "" {
		return errors.New("prom_ql is blank")
	}

	if ar.PromEvalInterval <= 0 {
		ar.PromEvalInterval = 15
	}

	// check in front-end
	// if _, err := parser.ParseExpr(ar.PromQl); err != nil {
	// 	return errors.New("prom_ql parse error: %")
	// }

	ar.AppendTags = strings.TrimSpace(ar.AppendTags)
	arr := strings.Fields(ar.AppendTags)
	for i := 0; i < len(arr); i++ {
		if len(strings.Split(arr[i], "=")) != 2 {
			return fmt.Errorf("AppendTags(%s) invalid", arr[i])
		}
	}

	gids := strings.Fields(ar.NotifyGroups)
	for i := 0; i < len(gids); i++ {
		if _, err := strconv.ParseInt(gids[i], 10, 64); err != nil {
			return fmt.Errorf("NotifyGroups(%s) invalid", ar.NotifyGroups)
		}
	}

	channels := strings.Fields(ar.NotifyChannels)
	if len(channels) > 0 {
		nlst := make([]string, 0, len(channels))
		for i := 0; i < len(channels); i++ {
			if config.LabelAndKeyHasKey(config.C.NotifyChannels, channels[i]) {
				nlst = append(nlst, channels[i])
			}
		}
		ar.NotifyChannels = strings.Join(nlst, " ")
	} else {
		ar.NotifyChannels = ""
	}

	return nil
}

func (ar *AlertRule) Add() error {
	if err := ar.Verify(); err != nil {
		return err
	}

	exists, err := AlertRuleExists(0, ar.GroupId, ar.Cluster, ar.Name)
	if err != nil {
		return err
	}

	if exists {
		return errors.New("AlertRule already exists")
	}

	now := time.Now().Unix()
	ar.CreateAt = now
	ar.UpdateAt = now

	return Insert(ar)
}

func (ar *AlertRule) Update(arf AlertRule) error {
	if ar.Name != arf.Name {
		exists, err := AlertRuleExists(ar.Id, ar.GroupId, ar.Cluster, arf.Name)
		if err != nil {
			return err
		}

		if exists {
			return errors.New("AlertRule already exists")
		}
	}

	err := arf.FE2DB()
	if err != nil {
		return err
	}

	arf.Id = ar.Id
	arf.GroupId = ar.GroupId
	arf.CreateAt = ar.CreateAt
	arf.CreateBy = ar.CreateBy
	arf.UpdateAt = time.Now().Unix()

	err = arf.Verify()
	if err != nil {
		return err
	}
	// return DB().Model(ar).Select("*").Updates(arf).Error
	ctx := getCtx()
	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, &arf)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (ar *AlertRule) UpdateFieldsMap(fields map[string]interface{}) error {
	// return DB().Model(ar).Updates(fields).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {

		_, err := zorm.UpdateNotZeroValue(ctx, ar)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (ar *AlertRule) FillNotifyGroups(cache map[int64]*UserGroup) error {
	// some user-group already deleted ?
	count := len(ar.NotifyGroupsJSON)
	if count == 0 {
		ar.NotifyGroupsObj = []UserGroup{}
		return nil
	}

	exists := make([]string, 0, count)
	delete := false
	for i := range ar.NotifyGroupsJSON {
		id, _ := strconv.ParseInt(ar.NotifyGroupsJSON[i], 10, 64)

		ug, has := cache[id]
		if has {
			exists = append(exists, ar.NotifyGroupsJSON[i])
			ar.NotifyGroupsObj = append(ar.NotifyGroupsObj, *ug)
			continue
		}

		ug, err := UserGroupGetById(id)
		if err != nil {
			return err
		}

		if ug == nil {
			delete = true
		} else {
			exists = append(exists, ar.NotifyGroupsJSON[i])
			ar.NotifyGroupsObj = append(ar.NotifyGroupsObj, *ug)
			cache[id] = ug
		}
	}

	if delete {
		ctx := getCtx()
		// some user-group already deleted
		ar.NotifyGroupsJSON = exists
		ar.NotifyGroups = strings.Join(exists, " ")
		// DB().Model(ar).Update("notify_groups", ar.NotifyGroups)
		finder := zorm.NewUpdateFinder(AlertRuleStructTableName)
		finder.Append(" notify_groups=? WHERE id=? ", ar.NotifyGroups, ar.Id)
		_, _ = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			_, err := zorm.UpdateFinder(ctx, finder)
			//如果返回的err不是nil,事务就会回滚
			return nil, err
		})
	}

	return nil
}

func (ar *AlertRule) FE2DB() error {
	ar.EnableDaysOfWeek = strings.Join(ar.EnableDaysOfWeekJSON, " ")
	ar.NotifyChannels = strings.Join(ar.NotifyChannelsJSON, " ")
	ar.NotifyGroups = strings.Join(ar.NotifyGroupsJSON, " ")
	ar.Callbacks = strings.Join(ar.CallbacksJSON, " ")
	ar.AppendTags = strings.Join(ar.AppendTagsJSON, " ")
	algoParamsByte, err := json.Marshal(ar.AlgoParamsJson)
	if err != nil {
		return fmt.Errorf("marshal algo_params err:%v", err)
	}

	ar.AlgoParams = string(algoParamsByte)
	return nil
}

func (ar *AlertRule) DB2FE() {
	ar.EnableDaysOfWeekJSON = strings.Fields(ar.EnableDaysOfWeek)
	ar.NotifyChannelsJSON = strings.Fields(ar.NotifyChannels)
	ar.NotifyGroupsJSON = strings.Fields(ar.NotifyGroups)
	ar.CallbacksJSON = strings.Fields(ar.Callbacks)
	ar.AppendTagsJSON = strings.Fields(ar.AppendTags)
	json.Unmarshal([]byte(ar.AlgoParams), &ar.AlgoParamsJson)
}

func AlertRuleDels(ids []int64, bgid ...int64) error {

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		var err error
		for i := 0; i < len(ids); i++ {
			// ret := DB().Where("id = ? and group_id=?", ids[i], busiGroupId).Delete(&AlertRule{})
			AlertRule := &AlertRule{}
			AlertRule.Id = ids[i]
			if len(bgid) > 0 {
				// session = session.Where("group_id = ?", bgid[0])
				AlertRule.GroupId = bgid[0]
			}
			_, err = zorm.Delete(ctx, AlertRule)
			if err != nil {
				return nil, err
			}
			// 说明确实删掉了，把相关的活跃告警也删了，这些告警永远都不会恢复了，而且策略都没了，说明没人关心了
			if err == nil {
				// DB().Where("rule_id = ?", ids[i]).Delete(new(AlertCurEvent))
				AlertCurEvent := &AlertCurEvent{}
				AlertCurEvent.RuleId = ids[i]
				_, err = zorm.Delete(ctx, AlertCurEvent)
			}
		}
		return nil, err
	})

	return err

}

func AlertRuleExists(id, groupId int64, cluster, name string) (bool, error) {
	ctx := getCtx()
	//session := DB().Where("id <> ? and group_id = ? and name = ?", id, groupId, name)
	finder := zorm.NewSelectFinder(AlertRuleStructTableName).Append("WHERE id <> ? and group_id = ? and name = ?", id, groupId, name)
	lst := make([]AlertRule, 0)
	//err := session.Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return false, err
	}
	if len(lst) == 0 {
		return false, nil
	}

	// match cluster
	for _, r := range lst {
		if MatchCluster(r.Cluster, cluster) {
			return true, nil
		}
	}
	return false, nil
}

func AlertRuleGets(groupId int64) ([]AlertRule, error) {
	// session := DB().Where("group_id=?", groupId).Order("name")
	lst := make([]AlertRule, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertRuleStructTableName) // select * from t_demo
	finder.Append(" Where group_id=?", groupId).Append(" Order by name")
	err := zorm.Query(ctx, finder, &lst, nil)
	// err := session.Find(&lst).Error
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}

	return lst, err
}

func AlertRuleGetsByCluster(cluster string) ([]*AlertRule, error) {
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertRuleStructTableName) // select * from t_demo
	// session := DB().Where("disabled = ? and prod = ?", 0, "")
	finder.Append(" Where disabled = ? and prod = ?", 0, "")
	if cluster != "" {
		finder.Append(" and (cluster like ? or cluster like ?)", "%"+cluster+"%", "%"+ClusterAll+"%")
	}

	lst := make([]*AlertRule, 0)
	//err := session.Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return lst, err
	}

	if len(lst) == 0 {
		return lst, nil
	}

	if cluster == "" {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
		return lst, nil
	}

	lr := make([]*AlertRule, 0, len(lst))
	for _, r := range lst {
		if MatchCluster(r.Cluster, cluster) {
			r.DB2FE()
			lr = append(lr, r)
		}
	}

	return lr, err
}

func AlertRulesGetsBy(prods []string, query string) ([]*AlertRule, error) {
	// session := DB().Where("disabled = ? and prod IN (?)", 0, prods)
	// err := session.Find(&lst).Error
	lst := make([]*AlertRule, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertRuleStructTableName) // select * from t_demo
	finder.Append(" Where disabled = ? and prod IN (?)", 0, prods)

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			// session = session.Where("append_tags like ?", qarg)
			finder.Append(" And append_tags like ?", qarg)
		}
	}

	err := zorm.Query(ctx, finder, &lst, nil)

	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}

	return lst, err
}

func AlertRuleGet(where string, args ...interface{}) (*AlertRule, error) {
	lst := make([]*AlertRule, 0)
	// err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(AlertRuleStructTableName) // select * from t_demo
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

	return lst[0], nil
}

func AlertRuleGetById(id int64) (*AlertRule, error) {
	return AlertRuleGet("id=?", id)
}

func AlertRuleGetName(id int64) (string, error) {
	names := make([]string, 0)
	ctx := getCtx()
	// err := DB().Model(new(AlertRule)).Where("id = ?", id).Pluck("name", &names).Error
	finder := zorm.NewSelectFinder(AlertRuleStructTableName, "name")
	err := zorm.Query(ctx, finder, &names, nil)
	if err != nil {
		return "", err
	}

	if len(names) == 0 {
		return "", nil
	}

	return names[0], nil
}

func AlertRuleStatistics(cluster string) (*Statistics, error) {
	// session := DB().Model(&AlertRule{}).Select("count(*) as total", "max(update_at) as last_updated").Where("disabled = ? and prod = ?", 0, "")

	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertRuleStructTableName, "count(*) as total, max(update_at) as last_updated")
	finder.Append(" Where disabled = ? and prod = ?", 0, "")
	if cluster != "" {
		//  简略的判断，当一个clustername是另一个clustername的substring的时候，会出现stats与预期不符，不影响使用
		finder.Append(" and (cluster like ? or cluster like ?)", "%"+cluster+"%", "%"+ClusterAll+"%")
	}

	stats := make([]*Statistics, 0)
	// err := session.Find(&stats).Error
	err := zorm.Query(ctx, finder, &stats, nil)

	if err != nil {
		return nil, err
	}

	return stats[0], nil
}
