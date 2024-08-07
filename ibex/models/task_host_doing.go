package models

import (
	"encoding/json"
	"fmt"
	"sync"

	"gitee.com/chunanyong/zorm"
)

const TaskHostDoingTableName = "task_host_doing"

type TaskHostDoing struct {
	zorm.EntityStruct
	Id             int64  `column:"id"`
	Host           string `column:"host"`
	Clock          int64  `column:"clock"`
	Action         string `column:"action"`
	AlertTriggered bool
}

func (TaskHostDoing) GetTableName() string {
	return TaskHostDoingTableName
}

func (doing *TaskHostDoing) MarshalBinary() ([]byte, error) {
	return json.Marshal(doing)
}

func (doing *TaskHostDoing) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, doing)
}

func hostDoingCacheKey(id int64, host string) string {
	return fmt.Sprintf("%s:%d", host, id)
}

var (
	doingLock sync.RWMutex
	doingMaps map[string][]TaskHostDoing
)

func SetDoingCache(v map[string][]TaskHostDoing) {
	doingLock.Lock()
	doingMaps = v
	doingLock.Unlock()
}

func GetDoingCache(host string) []TaskHostDoing {
	doingLock.RLock()
	defer doingLock.RUnlock()

	return doingMaps[host]
}

func CheckExistAndEdgeAlertTriggered(host string, id int64) (exist, isAlertTriggered bool) {
	doingLock.RLock()
	defer doingLock.RUnlock()

	doings := doingMaps[host]
	for _, doing := range doings {
		if doing.Id == id {
			exist = true
			isAlertTriggered = doing.AlertTriggered
			return
		}
	}

	return false, false
}
