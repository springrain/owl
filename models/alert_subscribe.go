package models

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/pkg/ormx"
	"github.com/ccfos/nightingale/v6/pkg/poster"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/logger"
)

const AlertSubscribeTableName = "alert_subscribe"

type AlertSubscribe struct {
	zorm.EntityStruct
	Id                int64        `json:"id" column:"id"`
	Name              string       `json:"name" column:"name"`         // AlertSubscribe name
	Disabled          int          `json:"disabled" column:"disabled"` // 0: enabled, 1: disabled
	GroupId           int64        `json:"group_id" column:"group_id"`
	Prod              string       `json:"prod" column:"prod"`
	Cate              string       `json:"cate" column:"cate"`
	DatasourceIds     string       `json:"-" column:"datasource_ids"` // datasource ids
	DatasourceIdsJson []int64      `json:"datasource_ids"`            // for fe
	Cluster           string       `json:"cluster" column:"cluster"`  // take effect by clusters, seperated by space
	RuleId            int64        `json:"rule_id" column:"rule_id"`
	Severities        string       `json:"-" column:"severities"`              // sub severity
	SeveritiesJson    []int        `json:"severities"`                         // for fe
	ForDuration       int64        `json:"for_duration" column:"for_duration"` // for duration, unit: second
	RuleName          string       `json:"rule_name"`                          // for fe
	Tags              ormx.JSONArr `json:"tags" column:"tags"`
	RedefineSeverity  int          `json:"redefine_severity" column:"redefine_severity"`
	NewSeverity       int          `json:"new_severity" column:"new_severity"`
	RedefineChannels  int          `json:"redefine_channels" column:"redefine_channels"`
	NewChannels       string       `json:"new_channels" column:"new_channels"`
	UserGroupIds      string       `json:"user_group_ids" column:"user_group_ids"`
	UserGroups        []UserGroup  `json:"user_groups"` // for fe
	RedefineWebhooks  int          `json:"redefine_webhooks" column:"redefine_webhooks"`
	Webhooks          string       `json:"-" column:"webhooks"`
	WebhooksJson      []string     `json:"webhooks"`
	ExtraConfig       string       `json:"-" column:"extra_config"`
	ExtraConfigJson   interface{}  `json:"extra_config"` // for fe
	Note              string       `json:"note" column:"note"`
	CreateBy          string       `json:"create_by" column:"create_by"`
	CreateAt          int64        `json:"create_at" column:"create_at"`
	UpdateBy          string       `json:"update_by" column:"update_by"`
	UpdateAt          int64        `json:"update_at" column:"update_at"`
	ITags             []TagFilter  `json:"-"` // inner tags
	BusiGroups        ormx.JSONArr `json:"busi_groups" column:"busi_groups"`
	IBusiGroups       []TagFilter  `json:"-"` // inner busiGroups
	RuleIdsJson       []int64      `json:"rule_ids"`
	RuleIds           string       `json:"-" column:"rule_ids"`
	RuleNames         []string     `json:"rule_names"`
}

func (s *AlertSubscribe) GetTableName() string {
	return "alert_subscribe"
}

func AlertSubscribeGets(ctx *ctx.Context, groupId int64) ([]AlertSubscribe, error) {
	lst := make([]AlertSubscribe, 0)
	finder := zorm.NewSelectFinder(AlertSubscribeTableName).Append("WHERE group_id=? order by id desc", groupId)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Where("group_id=?", groupId).Order("id desc").Find(&lst).Error
	return lst, err
	//err = DB(ctx).Where("group_id=?", groupId).Order("id desc").Find(&lst).Error
	//return
}

func AlertSubscribeGetsByBGIds(ctx *ctx.Context, bgids []int64) ([]AlertSubscribe, error) {
	lst := make([]AlertSubscribe, 0)
	finder := zorm.NewSelectFinder(AlertSubscribeTableName).Append("WHERE 1=1")
	//session := DB(ctx)
	if len(bgids) > 0 {
		//session = session.Where("group_id in (?)", bgids)
		finder.Append("and group_id in (?)", bgids)
	}
	finder.Append(" order by id desc")
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err = session.Order("id desc").Find(&lst).Error
	return lst, err
}

func AlertSubscribeGetsByService(ctx *ctx.Context) ([]AlertSubscribe, error) {
	lst := make([]AlertSubscribe, 0)
	finder := zorm.NewSelectFinder(AlertSubscribeTableName)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err = DB(ctx).Find(&lst).Error
	if err != nil {
		return lst, err
	}

	for i := range lst {
		lst[i].DB2FE()
	}
	return lst, err
}

func AlertSubscribeGet(ctx *ctx.Context, where string, args ...interface{}) (*AlertSubscribe, error) {
	lst := make([]*AlertSubscribe, 0)
	finder := zorm.NewSelectFinder(AlertSubscribeTableName)
	AppendWhere(finder, where, args...)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//var lst []*AlertSubscribe
	//err := DB(ctx).Where(where, args...).Find(&lst).Error
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}

func (s *AlertSubscribe) IsDisabled() bool {
	return s.Disabled == 1
}

func (s *AlertSubscribe) Verify() error {
	if IsAllDatasource(s.DatasourceIdsJson) {
		s.DatasourceIdsJson = []int64{0}
	}

	if err := s.Parse(); err != nil {
		return err
	}

	if len(s.SeveritiesJson) == 0 {
		return errors.New("severities is required")
	}

	if s.UserGroupIds != "" && s.NewChannels == "" {
		// 如果指定了用户组，那么新告警的通知渠道必须指定，否则容易出现告警规则中没有指定通知渠道，导致订阅通知时，没有通知渠道
		return errors.New("new_channels is required")
	}

	ugids := strings.Fields(s.UserGroupIds)
	for i := 0; i < len(ugids); i++ {
		if _, err := strconv.ParseInt(ugids[i], 10, 64); err != nil {
			return errors.New("user_group_ids invalid")
		}
	}

	return nil
}

func (s *AlertSubscribe) FE2DB() error {
	if len(s.DatasourceIdsJson) > 0 {
		idsByte, _ := json.Marshal(s.DatasourceIdsJson)
		s.DatasourceIds = string(idsByte)
	}

	if len(s.WebhooksJson) > 0 {
		b, _ := json.Marshal(s.WebhooksJson)
		s.Webhooks = string(b)
	}

	b, _ := json.Marshal(s.ExtraConfigJson)
	s.ExtraConfig = string(b)

	if len(s.SeveritiesJson) > 0 {
		b, _ := json.Marshal(s.SeveritiesJson)
		s.Severities = string(b)
	}
	if len(s.RuleIdsJson) > 0 {
		b, _ := json.Marshal(s.RuleIdsJson)
		s.RuleIds = string(b)
	}
	return nil
}

func (s *AlertSubscribe) DB2FE() error {
	if s.DatasourceIds != "" {
		if err := json.Unmarshal([]byte(s.DatasourceIds), &s.DatasourceIdsJson); err != nil {
			return err
		}
	}

	if s.Webhooks != "" {
		if err := json.Unmarshal([]byte(s.Webhooks), &s.WebhooksJson); err != nil {
			return err
		}
	}

	if s.ExtraConfig != "" {
		if err := json.Unmarshal([]byte(s.ExtraConfig), &s.ExtraConfigJson); err != nil {
			return err
		}
	}

	if s.Severities != "" {
		if err := json.Unmarshal([]byte(s.Severities), &s.SeveritiesJson); err != nil {
			return err
		}
	}
	if s.RuleIds != "" {
		if err := json.Unmarshal([]byte(s.RuleIds), &s.RuleIdsJson); err != nil {
			return err
		}
	}
	return nil
}

func (s *AlertSubscribe) Parse() error {
	var err error
	s.ITags, err = GetTagFilters(s.Tags)
	if err != nil {
		return err
	}
	s.IBusiGroups, err = GetTagFilters(s.BusiGroups)
	return err
}

func (s *AlertSubscribe) Add(ctx *ctx.Context) error {
	if err := s.Verify(); err != nil {
		return err
	}

	if err := s.FE2DB(); err != nil {
		return err
	}

	now := time.Now().Unix()
	s.CreateAt = now
	s.UpdateAt = now
	return Insert(ctx, s)
}

func (s *AlertSubscribe) CompatibleWithOldRuleId() {
	if len(s.RuleIdsJson) == 0 && s.RuleId != 0 {
		s.RuleIdsJson = append(s.RuleIdsJson, s.RuleId)
	}
}

func (s *AlertSubscribe) FillRuleNames(ctx *ctx.Context, cache map[int64]string) error {
	s.CompatibleWithOldRuleId()
	if len(s.RuleIdsJson) == 0 {
		return nil
	}

	idNameHas := make(map[int64]string, len(s.RuleIdsJson))
	idsNotInCache := make([]int64, 0)
	for _, rid := range s.RuleIdsJson {
		rname, exist := cache[rid]
		if exist {
			idNameHas[rid] = rname
		} else {
			idsNotInCache = append(idsNotInCache, rid)
		}
	}

	if len(idsNotInCache) > 0 {
		lst, err := AlertRuleGetsByIds(ctx, idsNotInCache)
		if err != nil {
			return err
		}
		for _, alterRule := range lst {
			idNameHas[alterRule.Id] = alterRule.Name
			cache[alterRule.Id] = alterRule.Name
		}
	}

	names := make([]string, len(s.RuleIdsJson))
	for i, rid := range s.RuleIdsJson {
		if name, exist := idNameHas[rid]; exist {
			names[i] = name
		} else if rid == 0 {
			names[i] = ""
		} else {
			names[i] = "Error: AlertRule not found"
		}
	}
	s.RuleNames = names

	return nil
}

// for v5 rule
func (s *AlertSubscribe) FillDatasourceIds(ctx *ctx.Context) error {
	if s.DatasourceIds != "" {
		json.Unmarshal([]byte(s.DatasourceIds), &s.DatasourceIdsJson)
		return nil
	}
	return nil
}

func (s *AlertSubscribe) FillUserGroups(ctx *ctx.Context, cache map[int64]*UserGroup) error {
	// some user-group already deleted ?
	ugids := strings.Fields(s.UserGroupIds)

	count := len(ugids)
	if count == 0 {
		s.UserGroups = []UserGroup{}
		return nil
	}

	exists := make([]string, 0, count)
	isDelete := false
	for i := range ugids {
		id, _ := strconv.ParseInt(ugids[i], 10, 64)

		ug, has := cache[id]
		if has {
			exists = append(exists, ugids[i])
			s.UserGroups = append(s.UserGroups, *ug)
			continue
		}

		ug, err := UserGroupGetById(ctx, id)
		if err != nil {
			return err
		}

		if ug == nil {
			isDelete = true
		} else {
			exists = append(exists, ugids[i])
			s.UserGroups = append(s.UserGroups, *ug)
			cache[id] = ug
		}
	}

	if isDelete {
		// some user-group already deleted
		//DB(ctx).Model(s).Update("user_group_ids", strings.Join(exists, " "))
		UpdateColumn(ctx, AlertSubscribeTableName, s.Id, "user_group_ids", strings.Join(exists, " "))
		s.UserGroupIds = strings.Join(exists, " ")
	}

	return nil
}

func (s *AlertSubscribe) Update(ctx *ctx.Context, selectField string, selectFields ...string) error {
	if err := s.Verify(); err != nil {
		return err
	}

	if err := s.FE2DB(); err != nil {
		return err
	}

	cols := make([]string, 0)
	cols = append(cols, selectField)
	cols = append(cols, selectFields...)
	return Update(ctx, s, cols)
	//return DB(ctx).Model(s).Select(selectField, selectFields...).Updates(s).Error
}

func AlertSubscribeDel(ctx *ctx.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	finder := zorm.NewDeleteFinder(AlertSubscribeTableName).Append("WHERE id in (?)", ids)
	return UpdateFinder(ctx, finder)
	//return DB(ctx).Where("id in ?", ids).Delete(new(AlertSubscribe)).Error
}

func AlertSubscribeStatistics(ctx *ctx.Context) (*Statistics, error) {
	if !ctx.IsCenter {
		s, err := poster.GetByUrls[*Statistics](ctx, "/v1/n9e/statistic?name=alert_subscribe")
		return s, err
	}

	return StatisticsGet(ctx, AlertSubscribeTableName)

	/*
		session := DB(ctx).Model(&AlertSubscribe{}).Select("count(*) as total", "max(update_at) as last_updated")

		var stats []*Statistics
		err := session.Find(&stats).Error
		if err != nil {
			return nil, err
		}

		return stats[0], nil
	*/
}

func AlertSubscribeGetsAll(ctx *ctx.Context) ([]*AlertSubscribe, error) {
	if !ctx.IsCenter {
		lst, err := poster.GetByUrls[[]*AlertSubscribe](ctx, "/v1/n9e/alert-subscribes")
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(lst); i++ {
			lst[i].FE2DB()
		}
		return lst, err
	}

	// get my cluster's subscribes
	//session := DB(ctx).Model(&AlertSubscribe{})

	lst := make([]*AlertSubscribe, 0)
	//err := session.Find(&lst).Error
	finder := zorm.NewSelectFinder(AlertSubscribeTableName)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	return lst, err
	//return lst, err
}

func (s *AlertSubscribe) MatchProd(prod string) bool {
	//Replace 'prod' with optional item
	if s.Prod == "" {
		return true
	}
	return s.Prod == prod
}

func (s *AlertSubscribe) MatchCluster(dsId int64) bool {
	// 没有配置数据源, 或者事件不需要关联数据源
	// do not match any datasource or event not related to datasource
	if len(s.DatasourceIdsJson) == 0 || dsId == 0 {
		return true
	}

	for _, id := range s.DatasourceIdsJson {
		if id == dsId || id == 0 {
			return true
		}
	}
	return false
}

func (s *AlertSubscribe) ModifyEvent(event *AlertCurEvent) {
	if s.RedefineSeverity == 1 {
		event.Severity = s.NewSeverity
	}

	if s.RedefineChannels == 1 {
		event.NotifyChannels = s.NewChannels
		event.NotifyChannelsJSON = strings.Fields(s.NewChannels)
	}

	if s.RedefineWebhooks == 1 {
		event.Callbacks = s.Webhooks
		event.CallbacksJSON = s.WebhooksJson
	} else {
		// 将 callback 重置为空，防止事件被订阅之后，再次将事件发送给回调地址
		event.Callbacks = ""
		event.CallbacksJSON = []string{}
	}

	event.NotifyGroups = s.UserGroupIds
	event.NotifyGroupsJSON = strings.Fields(s.UserGroupIds)
}

func (s *AlertSubscribe) UpdateFieldsMap(ctx *ctx.Context, fields map[string]interface{}) error {
	return UpdateFieldsMap(ctx, s, s.Id, fields)
	//return DB(ctx).Model(s).Updates(fields).Error
}

func AlertSubscribeUpgradeToV6(ctx *ctx.Context, dsm map[string]Datasource) error {
	lst := make([]*AlertSubscribe, 0)
	finder := zorm.NewSelectFinder(AlertSubscribeTableName)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Find(&lst).Error
	if err != nil {
		return err
	}

	for i := 0; i < len(lst); i++ {
		var ids []int64
		if lst[i].Cluster == "$all" {
			ids = append(ids, 0)
		} else {
			clusters := strings.Fields(lst[i].Cluster)
			for j := 0; j < len(clusters); j++ {
				if ds, exists := dsm[clusters[j]]; exists {
					ids = append(ids, ds.Id)
				}
			}
		}
		b, err := json.Marshal(ids)
		if err != nil {
			continue
		}
		lst[i].DatasourceIds = string(b)

		if lst[i].Prod == "" {
			lst[i].Prod = METRIC
		}

		if lst[i].Cate == "" {
			lst[i].Cate = PROMETHEUS
		}

		err = lst[i].UpdateFieldsMap(ctx, map[string]interface{}{
			"datasource_ids": lst[i].DatasourceIds,
			"prod":           lst[i].Prod,
			"cate":           PROMETHEUS,
		})
		if err != nil {
			logger.Errorf("update alert rule:%d datasource ids failed, %v", lst[i].Id, err)
		}
	}
	return nil
}
