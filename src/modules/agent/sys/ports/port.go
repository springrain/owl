package ports

import (
	"github.com/didi/nightingale/src/models"
)

var (
	Ports              = make(map[int]*models.PortCollect)
	PortsWithScheduler = make(map[int]*PortScheduler)
)

func samePort(new, old *models.PortCollect) bool {
	if new.Step != old.Step {
		return false
	}

	if new.Tags != old.Tags {
		return false
	}

	if new.Timeout != old.Timeout {
		return false
	}

	if new.Port != old.Port {
		return false
	}

	return true
}

func DelNoPortCollect(newCollect map[int]*models.PortCollect) {
	for currKey := range Ports {
		newPort, ok := newCollect[currKey]
		if !ok || !samePort(newPort, Ports[currKey]) {
			deletePort(currKey)
		}
	}
}

func AddNewPortCollect(newCollect map[int]*models.PortCollect) {
	for target, newPort := range newCollect {
		if _, ok := Ports[target]; ok && samePort(newPort, Ports[target]) {
			continue
		}

		Ports[target] = newPort
		sch := NewPortScheduler(newPort)
		PortsWithScheduler[target] = sch
		sch.Schedule()
	}
}

func deletePort(key int) {
	v, ok := PortsWithScheduler[key]
	if ok {
		v.Stop()
		delete(PortsWithScheduler, key)
	}
	delete(Ports, key)
}
