package procs

import (
	"github.com/didi/nightingale/src/models"
)

var (
	Procs              = make(map[string]*models.ProcCollect)
	ProcsWithScheduler = make(map[string]*ProcScheduler)
)

func sameProc(new, old *models.ProcCollect) bool {
	if new.CollectMethod != old.CollectMethod {
		return false
	}

	if new.Step != old.Step {
		return false
	}

	if new.Target != old.Target {
		return false
	}

	if new.Tags != old.Tags {
		return false
	}

	return true
}

func DelNoProcCollect(newCollect map[string]*models.ProcCollect) {
	for currKey := range Procs {
		newProc, ok := newCollect[currKey]
		if !ok || !sameProc(newProc, Procs[currKey]) {
			deleteProc(currKey)
		}
	}
}

func AddNewProcCollect(newCollect map[string]*models.ProcCollect) {
	for target, newProc := range newCollect {
		if _, ok := Procs[target]; ok && sameProc(newProc, Procs[target]) {
			continue
		}

		Procs[target] = newProc
		sch := NewProcScheduler(newProc)
		ProcsWithScheduler[target] = sch
		sch.Schedule()
	}
}

func deleteProc(key string) {
	v, ok := ProcsWithScheduler[key]
	if ok {
		v.Stop()
		delete(ProcsWithScheduler, key)
	}
	delete(Procs, key)
}
