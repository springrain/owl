package models

import (
	"context"
	"log"
	"sort"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/pkg/poster"
	"golang.org/x/exp/slices"

	"github.com/pkg/errors"
	"github.com/toolkits/pkg/container/set"
)

const TargetTableName = "target"

type TargetDeleteHookFunc func(ctx *ctx.Context, idents []string) error
type Target struct {
	// 引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id           int64             `json:"id" column:"id"`
	GroupId      int64             `json:"group_id" column:"group_id"`
	GroupObjs    []*BusiGroup      `json:"group_objs"`
	Ident        string            `json:"ident" column:"ident"`
	Note         string            `json:"note" column:"note"`
	Tags         string            `json:"-" column:"tags"`
	TagsJSON     []string          `json:"tags"`
	TagsMap      map[string]string `json:"tags_maps"` // internal use, append tags to series
	UpdateAt     int64             `json:"update_at" column:"update_at"`
	HostIp       string            `json:"host_ip" column:"host_ip"` //ipv4，do not needs range select
	AgentVersion string            `json:"agent_version" column:"agent_version"`
	EngineName   string            `json:"engine_name" column:"engine_name"`
	OS           string            `json:"os"`
	HostTags     string            `json:"-" column:"host_tags"`
	HostTagsJson []string          `json:"host_tags"`
	UnixTime     int64             `json:"unixtime"`
	Offset       int64             `json:"offset"`
	TargetUp     float64           `json:"target_up"`
	MemUtil      float64           `json:"mem_util"`
	CpuNum       int               `json:"cpu_num"`
	CpuUtil      float64           `json:"cpu_util"`
	Arch         string            `json:"arch"`
	RemoteAddr   string            `json:"remote_addr"`
	GroupIds     []int64           `json:"group_ids"`
	GroupNames   []string          `json:"group_names"`
}

func (t *Target) GetTableName() string {
	return TargetTableName
}

func (t *Target) FillGroup(ctx *ctx.Context, cache map[int64]*BusiGroup) error {
	var err error
	if len(t.GroupIds) == 0 {
		t.GroupIds, err = TargetGroupIdsGetByIdent(ctx, t.Ident)
		if err != nil {
			return errors.WithMessage(err, "failed to get target gids")
		}
		t.GroupObjs = make([]*BusiGroup, 0, len(t.GroupIds))
	}

	for _, gid := range t.GroupIds {
		bg, has := cache[gid]
		if has && bg != nil {
			t.GroupObjs = append(t.GroupObjs, bg)
			continue
		}

		bg, err := BusiGroupGetById(ctx, gid)
		if err != nil {
			return errors.WithMessage(err, "failed to get busi group")
		}

		if bg == nil {
			continue
		}

		t.GroupObjs = append(t.GroupObjs, bg)
		cache[gid] = bg
	}

	return nil
}

func (t *Target) MatchGroupId(gid ...int64) bool {
	for _, tgId := range t.GroupIds {
		for _, id := range gid {
			if tgId == id {
				return true
			}
		}
	}
	return false
}

func (t *Target) AfterFind(tx *zorm.Finder) (err error) {
	delta := time.Now().Unix() - t.UpdateAt
	if delta < 60 {
		t.TargetUp = 2
	} else if delta < 180 {
		t.TargetUp = 1
	}
	t.FillTagsMap()
	return
}

func TargetStatistics(ctx *ctx.Context) (*Statistics, error) {
	if !ctx.IsCenter {
		s, err := poster.GetByUrls[*Statistics](ctx, "/v1/n9e/statistic?name=target")
		return s, err
	}
	return StatisticsGet(ctx, TargetTableName)
	/*
		var stats []*Statistics
		err := DB(ctx).Model(&Target{}).Select("count(*) as total", "max(update_at) as last_updated").Find(&stats).Error
		if err != nil {
			return nil, err
		}

		return stats[0], nil
	*/
}

func TargetDel(ctx *ctx.Context, idents []string, deleteHook TargetDeleteHookFunc) error {
	if len(idents) == 0 {
		panic("idents empty")
	}

	_, err := zorm.Transaction(ctx.Ctx, func(ctxC context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(TargetTableName).Append("WHERE ident in (?)", idents)
		_, err := zorm.UpdateFinder(ctxC, finder)
		if err != nil {
			return nil, err
		}
		err = deleteHook(ctx, idents)
		if err != nil {
			return nil, err
		}
		err = TargetDeleteBgids(ctx, idents)
		return nil, err
	})

	return err

}

type BuildTargetWhereOption func(finder *zorm.Finder) *zorm.Finder

func BuildTargetWhereWithBgids(bgids []int64) BuildTargetWhereOption {
	return func(finder *zorm.Finder) *zorm.Finder {
		if len(bgids) == 1 && bgids[0] == 0 {
			finder.Append("left join target_busi_group on target.ident = target_busi_group.target_ident WHERE target_busi_group.target_ident is null")
		} else if len(bgids) > 0 {
			if slices.Contains(bgids, 0) {
				finder.Append("left join target_busi_group on target.ident = target_busi_group.target_ident WHERE target_busi_group.target_ident is null OR target_busi_group.group_id in (?)", bgids)
			} else {
				finder.Append("join target_busi_group on target.ident = target_busi_group.target_ident WHERE target_busi_group.group_id in (?)", bgids)
			}
		}
		return finder
	}
}

func BuildTargetWhereWithDsIds(dsIds []int64) BuildTargetWhereOption {
	return func(finder *zorm.Finder) *zorm.Finder {
		if len(dsIds) > 0 {
			finder = finder.Append(" and datasource_id in (?)", dsIds)
		}
		return finder
	}
}

func BuildTargetWhereWithHosts(hosts []string) BuildTargetWhereOption {
	return func(finder *zorm.Finder) *zorm.Finder {
		if len(hosts) > 0 {
			finder = finder.Append(" and (ident in (?) or host_ip in (?))", hosts, hosts)
		}
		return finder
	}
}

func BuildTargetWhereWithQuery(query string) BuildTargetWhereOption {
	return func(finder *zorm.Finder) *zorm.Finder {
		if query != "" {
			arr := strings.Fields(query)
			for i := 0; i < len(arr); i++ {
				q := "%" + arr[i] + "%"
				finder = finder.Append(" and (ident like ? or host_ip like ? or note like ? or tags like ? or host_tags like ? or os like ?)", q, q, q, q, q, q)
			}
		}
		return finder
	}
}

func BuildTargetWhereWithDowntime(downtime int64) BuildTargetWhereOption {
	return func(finder *zorm.Finder) *zorm.Finder {
		if downtime > 0 {
			finder = finder.Append(" and target.update_at < ?", time.Now().Unix()-downtime)
		}
		return finder
	}
}

func buildTargetWhere(ctx *ctx.Context, selectField string, options ...BuildTargetWhereOption) *zorm.Finder {
	finder := zorm.NewSelectFinder(TargetTableName, selectField)
	//session := DB(ctx).Model(&Target{})
	finder.SelectTotalCount = false

	for _, opt := range options {
		finder = opt(finder)
	}

	return finder
}

func TargetTotalCount(ctx *ctx.Context) (int64, error) {
	finder := zorm.NewSelectFinder(TargetTableName, "count(*)")
	return Count(ctx, finder)
	//return Count(DB(ctx).Model(new(Target)))
}

func TargetTotal(ctx *ctx.Context, options ...BuildTargetWhereOption) (int64, error) {
	finder := buildTargetWhere(ctx, "count(*)", options...)
	return Count(ctx, finder)
	//return Count(buildTargetWhere(ctx, bgids, dsIds, query, downtime))
}

func TargetGets(ctx *ctx.Context, limit, offset int, order string, desc bool, options ...BuildTargetWhereOption) ([]*Target, error) {
	lst := make([]*Target, 0)
	if desc {
		order += " desc"
	} else {
		order += " asc"
	}
	finder := buildTargetWhere(ctx, "*", options...)
	finder.Append(" order by " + order)
	page := zorm.NewPage()
	page.PageSize = limit
	page.PageNo = offset / limit
	err := zorm.Query(ctx.Ctx, finder, &lst, page)
	return lst, err
}

// 根据 groupids, tags, hosts 查询 targets
func TargetGetsByFilter(ctx *ctx.Context, query []map[string]interface{}, limit, offset int) ([]*Target, error) {
	lst := make([]*Target, 0)
	finder, page := TargetFilterQueryBuild(ctx, "target.*", query, limit, offset)
	finder.Append("order by target.ident asc ")
	err := zorm.Query(ctx.Ctx, finder, &lst, page)
	cache := make(map[int64]*BusiGroup)
	for i := 0; i < len(lst); i++ {
		lst[i].TagsJSON = strings.Fields(lst[i].Tags)
		lst[i].HostTagsJson = strings.Fields(lst[i].HostTags)
		lst[i].FillGroup(ctx, cache)
	}

	return lst, err
}

func TargetCountByFilter(ctx *ctx.Context, query []map[string]interface{}) (int64, error) {
	finder, _ := TargetFilterQueryBuild(ctx, "count(*)", query, 0, 0)
	return Count(ctx, finder)
	//return Count(session)
}

func MissTargetGetsByFilter(ctx *ctx.Context, query []map[string]interface{}, ts int64) ([]*Target, error) {
	lst := make([]*Target, 0)
	finder, page := TargetFilterQueryBuild(ctx, "target.*", query, 0, 0)
	finder.Append("and target.update_at < ?", ts)
	//session = session.Where("update_at < ?", ts)
	finder.Append("order by target.ident asc")
	err := zorm.Query(ctx.Ctx, finder, &lst, page)
	//err := session.Order("ident").Find(&lst).Error
	return lst, err
}

func MissTargetCountByFilter(ctx *ctx.Context, query []map[string]interface{}, ts int64) (int64, error) {
	finder, _ := TargetFilterQueryBuild(ctx, "count(*)", query, 0, 0)
	finder.Append("and target.update_at < ?", ts)
	return Count(ctx, finder)
	//session = session.Where("update_at < ?", ts)
	//return Count(session)
}

func TargetFilterQueryBuild(ctx *ctx.Context, selectField string, query []map[string]interface{}, limit, offset int) (*zorm.Finder, *zorm.Page) {
	finder := zorm.NewSelectFinder(TargetTableName, selectField).Append("left join " +
		"target_busi_group on target.ident = target_busi_group.target_ident").Append("WHERE 1=1")
	finder.SelectTotalCount = false
	if len(query) > 0 {
		finder.Append("and (1=1 ")
	}
	//session := DB(ctx).Model(&Target{})
	for _, q := range query {
		//tx := DB(ctx).Model(&Target{})
		for k, v := range q {
			//tx = tx.Or(k, v)
			finder.Append("or "+k+"=?", v)
		}
		//session = session.Where(tx)
	}
	if len(query) > 0 {
		finder.Append(")")
	}
	var page *zorm.Page

	//session := DB(ctx).Model(&Target{}).Where("ident in (?)", sub)

	if limit > 0 {
		page = zorm.NewPage()
		page.PageSize = limit
		page.PageNo = offset / limit
		//session = session.Limit(limit).Offset(offset)
	}

	return finder, page
}

func TargetGetsAll(ctx *ctx.Context) ([]*Target, error) {
	if !ctx.IsCenter {
		lst, err := poster.GetByUrls[[]*Target](ctx, "/v1/n9e/targets")
		return lst, err
	}

	lst := make([]*Target, 0)
	finder := zorm.NewSelectFinder(TargetTableName)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Model(&Target{}).Find(&lst).Error
	if err != nil {
		return lst, err
	}

	tgs, err := TargetBusiGroupsGetAll(ctx)
	if err != nil {
		return lst, err
	}

	for i := 0; i < len(lst); i++ {
		lst[i].FillTagsMap()
		lst[i].GroupIds = tgs[lst[i].Ident]
	}

	return lst, err
}

func TargetUpdateNote(ctx *ctx.Context, idents []string, note string) error {
	finder := zorm.NewUpdateFinder(TargetTableName).Append("note=?,update_at=? WHERE ident in (?)", note, time.Now().Unix(), idents)
	return UpdateFinder(ctx, finder)
	/*
		return DB(ctx).Model(&Target{}).Where("ident in ?", idents).Updates(map[string]interface{}{
			"note":      note,
			"update_at": time.Now().Unix(),
		}).Error
	*/
}

func TargetUpdateBgid(ctx *ctx.Context, idents []string, bgid int64, clearTags bool) error {

	finder := zorm.NewUpdateFinder(TargetTableName).Append("group_id=?,update_at=?", bgid, time.Now().Unix())

	/*
		fields := map[string]interface{}{
			"group_id":  bgid,
			"update_at": time.Now().Unix(),
		}
	*/
	if clearTags {
		//fields["tags"] = ""
		finder.Append(",tags=?", "")
	}
	finder.Append("WHERE ident in (?)", idents)
	return UpdateFinder(ctx, finder)
	//return DB(ctx).Model(&Target{}).Where("ident in ?", idents).Updates(fields).Error
}

func TargetGet(ctx *ctx.Context, where string, args ...interface{}) (*Target, error) {
	lst := make([]*Target, 0)
	finder := zorm.NewSelectFinder(TargetTableName)
	AppendWhere(finder, where, args...)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Where(where, args...).Find(&lst).Error
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	lst[0].TagsJSON = strings.Fields(lst[0].Tags)
	lst[0].HostTagsJson = strings.Fields(lst[0].HostTags)

	return lst[0], nil
}

func TargetGetById(ctx *ctx.Context, id int64) (*Target, error) {
	return TargetGet(ctx, "id = ?", id)
}

func TargetGetByIdent(ctx *ctx.Context, ident string) (*Target, error) {
	return TargetGet(ctx, "ident = ?", ident)
}

func TargetsGetByIdents(ctx *ctx.Context, idents []string) ([]*Target, error) {
	targets := make([]*Target, 0)
	finder := zorm.NewSelectFinder(TargetTableName).Append("WHERE idents IN (?)", idents)
	err := zorm.Query(ctx.Ctx, finder, &targets, nil)
	//var targets []*Target
	//err := DB(ctx).Where("ident IN ?", idents).Find(&targets).Error
	return targets, err
}

func TargetsGetIdentsByIdentsAndHostIps(ctx *ctx.Context, idents, hostIps []string) (map[string]string, []string, error) {
	inexistence := make(map[string]string)
	identSet := set.NewStringSet()

	// Query the ident corresponding to idents
	if len(idents) > 0 {
		var identsFromIdents []string
		finder := zorm.NewSelectFinder(TargetTableName, "ident").Append("WHERE  ident in (?)", idents)
		err := zorm.Query(ctx.Ctx, finder, &identsFromIdents, nil)
		//err := DB(ctx).Model(&Target{}).Where("ident IN ?", idents).Pluck("ident", &identsFromIdents).Error
		if err != nil {
			return nil, nil, err
		}

		for _, ident := range identsFromIdents {
			identSet.Add(ident)
		}

		for _, ident := range idents {
			if !identSet.Exists(ident) {
				inexistence[ident] = "Ident not found"
			}
		}
	}

	// Query the hostIp corresponding to idents
	if len(hostIps) > 0 {
		var hostIpToIdentMap []struct {
			HostIp string
			Ident  string
		}
		finder := zorm.NewSelectFinder(TargetTableName, "host_ip, ident").Append("WHERE  host_ip in (?)", hostIps)
		err := zorm.Query(ctx.Ctx, finder, &hostIpToIdentMap, nil)
		//err := DB(ctx).Model(&Target{}).Select("host_ip, ident").Where("host_ip IN ?", hostIps).Scan(&hostIpToIdentMap).Error
		if err != nil {
			return nil, nil, err
		}

		hostIpToIdent := set.NewStringSet()
		for _, entry := range hostIpToIdentMap {
			hostIpToIdent.Add(entry.HostIp)
			identSet.Add(entry.Ident)
		}

		for _, hostIp := range hostIps {
			if !hostIpToIdent.Exists(hostIp) {
				inexistence[hostIp] = "HostIp not found"
			}
		}
	}

	return inexistence, identSet.ToSlice(), nil
}

func TargetGetTags(ctx *ctx.Context, idents []string, ignoreHostTag bool) ([]string, error) {
	finder := zorm.NewSelectFinder(TargetTableName, "tags,host_tags")
	//session := DB(ctx).Model(new(Target))

	arr := make([]*Target, 0)
	if len(idents) > 0 {
		//session = session.Where("ident in ?", idents)
		finder.Append("WHERE ident in (?)", idents)
	}
	err := zorm.Query(ctx.Ctx, finder, &arr, nil)
	//err := session.Select("distinct(tags) as tags").Pluck("tags", &arr).Error
	if err != nil {
		return nil, err
	}

	cnt := len(arr)
	if cnt == 0 {
		return []string{}, nil
	}

	set := make(map[string]struct{})
	for i := 0; i < cnt; i++ {
		tags := strings.Fields(arr[i].Tags)
		for j := 0; j < len(tags); j++ {
			set[tags[j]] = struct{}{}
		}

		if !ignoreHostTag {
			for _, ht := range arr[i].HostTagsJson {
				set[ht] = struct{}{}
			}
		}
	}

	cnt = len(set)
	ret := make([]string, 0, cnt)
	for key := range set {
		ret = append(ret, key)
	}

	sort.Strings(ret)

	return ret, err
}

func (t *Target) AddTags(ctx *ctx.Context, tags []string) error {
	for i := 0; i < len(tags); i++ {
		if !strings.Contains(t.Tags, tags[i]+" ") {
			t.Tags += tags[i] + " "
		}
	}

	arr := strings.Fields(t.Tags)
	sort.Strings(arr)

	finder := zorm.NewUpdateFinder(TargetTableName).Append("tags=?,update_at=? WHERE id=?", strings.Join(arr, " ")+" ", time.Now().Unix(), t.Id)
	return UpdateFinder(ctx, finder)
	/*
		return DB(ctx).Model(t).Updates(map[string]interface{}{
			"tags":      strings.Join(arr, " ") + " ",
			"update_at": time.Now().Unix(),
		}).Error
	*/
}

func (t *Target) DelTags(ctx *ctx.Context, tags []string) error {
	for _, tag := range tags {
		t.Tags = strings.ReplaceAll(t.Tags, tag+" ", "")
	}
	finder := zorm.NewUpdateFinder(TargetTableName).Append("tags=?,update_at=? WHERE id=?", t.Tags, time.Now().Unix(), t.Id)
	return UpdateFinder(ctx, finder)
	/*
		return DB(ctx).Model(t).Updates(map[string]interface{}{
			"tags":      t.Tags,
			"update_at": time.Now().Unix(),
		}).Error
	*/
}

func (t *Target) FillTagsMap() {
	t.TagsJSON = strings.Fields(t.Tags)
	t.TagsMap = make(map[string]string)
	m := make(map[string]string)
	allTags := append(t.TagsJSON, t.HostTagsJson...)
	for _, item := range allTags {
		arr := strings.Split(item, "=")
		if len(arr) != 2 {
			continue
		}
		m[arr[0]] = arr[1]
	}

	t.TagsMap = m
}

func (t *Target) GetTagsMap() map[string]string {
	tagsJSON := strings.Fields(t.Tags)
	m := make(map[string]string)
	for _, item := range tagsJSON {
		if arr := strings.Split(item, "="); len(arr) == 2 {
			m[arr[0]] = arr[1]
		}
	}
	return m
}

func (t *Target) GetHostTagsMap() map[string]string {
	m := make(map[string]string)
	for _, item := range t.HostTagsJson {
		arr := strings.Split(item, "=")
		if len(arr) != 2 {
			continue
		}
		m[arr[0]] = arr[1]
	}
	return m
}

func (t *Target) FillMeta(meta *HostMeta) {
	t.MemUtil = meta.MemUtil
	t.CpuUtil = meta.CpuUtil
	t.CpuNum = meta.CpuNum
	t.UnixTime = meta.UnixTime
	t.Offset = meta.Offset
	t.Arch = meta.Arch
	t.RemoteAddr = meta.RemoteAddr
}

func TargetIdents(ctx *ctx.Context, ids []int64) ([]string, error) {
	ret := make([]string, 0)

	if len(ids) == 0 {
		return ret, nil
	}
	finder := zorm.NewSelectFinder(TargetTableName, "ident").Append("WHERE id in (?)", ids)
	err := zorm.Query(ctx.Ctx, finder, &ret, nil)

	//err := DB(ctx).Model(&Target{}).Where("id in ?", ids).Pluck("ident", &ret).Error
	return ret, err
}

func TargetIds(ctx *ctx.Context, idents []string) ([]int64, error) {
	ret := make([]int64, 0)

	if len(idents) == 0 {
		return ret, nil
	}
	finder := zorm.NewSelectFinder(TargetTableName, "id").Append("WHERE ident in (?)", idents)
	err := zorm.Query(ctx.Ctx, finder, &ret, nil)

	//err := DB(ctx).Model(&Target{}).Where("ident in ?", idents).Pluck("id", &ret).Error
	return ret, err
}

func IdentsFilter(ctx *ctx.Context, idents []string, where string, args ...interface{}) ([]string, error) {
	arr := make([]string, 0)
	if len(idents) == 0 {
		return arr, nil
	}
	finder := zorm.NewSelectFinder(TargetTableName, "ident").Append("WHERE ident in (?)", idents)
	if where != "" {
		finder.Append("and "+where, args...)
	}

	err := zorm.Query(ctx.Ctx, finder, &arr, nil)
	//err := DB(ctx).Model(&Target{}).Where("ident in ?", idents).Where(where, args...).Pluck("ident", &arr).Error
	return arr, err
}

func (m *Target) UpdateFieldsMap(ctx *ctx.Context, fields map[string]interface{}) error {

	entityMap := zorm.NewEntityMap(TargetTableName)
	entityMap.PkColumnName = m.GetPKColumnName()
	for k, v := range fields {
		entityMap.Set(k, v)
	}
	entityMap.Set(m.GetPKColumnName(), m.Id)
	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		return zorm.UpdateEntityMap(ctx, entityMap)
	})
	return err
	//return DB(ctx).Model(m).Updates(fields).Error
}

// 1. 是否可以进行 busi_group 迁移
func CanMigrateBg(ctx *ctx.Context) bool {
	// 1.1 检查 target 表是否为空
	var cnt int64
	finder := zorm.NewSelectFinder(TargetTableName, "count(*)")
	cnt, err := Count(ctx, finder)
	if err != nil {
		log.Println("failed to get target table count, err:", err)
		return false
	}
	if cnt == 0 {
		log.Println("target table is empty, skip migration.")
		return false
	}

	// 1.2 判断是否已经完成迁移
	var maxGroupId int64
	finderMax := zorm.NewSelectFinder(TargetTableName, "MAX(group_id)")
	_, err = zorm.QueryRow(ctx.Ctx, finderMax, &maxGroupId)
	if err != nil {
		log.Println("failed to get max group_id from target table, err:", err)
		return false
	}

	if maxGroupId == 0 {
		return false
	}

	return true
}

func MigrateBg(ctx *ctx.Context, bgLabelKey string) {
	err := DoMigrateBg(ctx, bgLabelKey)
	if err != nil {
		log.Println("failed to migrate bgid, err:", err)
		return
	}

	log.Println("migration bgid has been completed")
}

func DoMigrateBg(ctx *ctx.Context, bgLabelKey string) error {
	// 2. 获取全量 target
	targets, err := TargetGetsAll(ctx)
	if err != nil {
		return err
	}

	// 3. 获取全量 busi_group
	bgs, err := BusiGroupGetAll(ctx)
	if err != nil {
		return err
	}

	bgById := make(map[int64]*BusiGroup, len(bgs))
	for _, bg := range bgs {
		bgById[bg.Id] = bg
	}

	// 4. 如果某 busi_group 有 label，将其存至对应的 target tags 中
	for _, t := range targets {
		if t.GroupId == 0 {
			continue
		}
		_, err := zorm.Transaction(ctx.Ctx, func(txctx context.Context) (interface{}, error) {
			// 4.1 将 group_id 迁移至关联表
			if err := TargetBindBgids(ctx, []string{t.Ident}, []int64{t.GroupId}); err != nil {
				return nil, err
			}
			if err := TargetUpdateBgid(ctx, []string{t.Ident}, 0, false); err != nil {
				return nil, err
			}

			// 4.2 判断该机器是否需要新增 tag
			if bg, ok := bgById[t.GroupId]; !ok || bg.LabelEnable == 0 ||
				strings.Contains(t.Tags, bgLabelKey+"=") {
				return nil, err
			} else {
				return nil, t.AddTags(ctx, []string{bgLabelKey + "=" + bg.LabelValue})
			}
		})
		if err != nil {
			log.Printf("failed to migrate %v bg, err: %v\n", t.Ident, err)
			continue
		}
	}
	return nil
}
