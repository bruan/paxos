package main

import "time"

type timer struct {
	id      int
	timeout time.Time
	f       func(int)
}

type timerMgr struct {
	ts        map[int]timer
	updateing bool
	updateAdd []timer
	updateDel []int
}

func newTimerMgr() *timerMgr {
	tm := timerMgr{}
	tm.ts = make(map[int]timer)

	return &tm
}

func (t *timerMgr) addTimer(id int, timeout time.Duration, f func(int)) {
	if !t.updateing {
		t.ts[id] = timer{id: id, timeout: time.Now().Add(timeout), f: f}
	} else {
		t.updateAdd = append(t.updateAdd, timer{id: id, timeout: time.Now().Add(timeout), f: f})
	}
}

func (t *timerMgr) delTimer(id int) {
	if !t.updateing {
		delete(t.ts, id)
	} else {
		t.updateDel = append(t.updateDel, id)
	}
}

func (t *timerMgr) update() {
	now := time.Now()
	t.updateing = true
	for k, v := range t.ts {
		if now.After(v.timeout) {
			v.f(k)
			t.updateDel = append(t.updateDel, k)
		}
	}
	t.updateing = false

	for _, id := range t.updateDel {
		delete(t.ts, id)
	}
	t.updateDel = nil
	for _, v := range t.updateAdd {
		t.ts[v.id] = timer{id: v.id, timeout: v.timeout, f: v.f}
	}
	t.updateAdd = nil
}
