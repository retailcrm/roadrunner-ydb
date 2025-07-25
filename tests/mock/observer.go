package mocklogger

import (
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

type LoggedEntry struct {
	zapcore.Entry
	Context []zapcore.Field
}

func (e LoggedEntry) ContextMap() map[string]any {
	encoder := zapcore.NewMapObjectEncoder()
	for _, f := range e.Context {
		f.AddTo(encoder)
	}
	return encoder.Fields
}

type ObservedLogs struct {
	mu   sync.RWMutex
	logs []LoggedEntry
}

func (o *ObservedLogs) Len() int {
	o.mu.RLock()
	n := len(o.logs)
	o.mu.RUnlock()
	return n
}

func (o *ObservedLogs) All() []LoggedEntry {
	o.mu.RLock()
	ret := make([]LoggedEntry, len(o.logs))
	copy(ret, o.logs)
	o.mu.RUnlock()
	return ret
}

func (o *ObservedLogs) TakeAll() []LoggedEntry {
	o.mu.Lock()
	ret := o.logs
	o.logs = nil
	o.mu.Unlock()
	return ret
}

func (o *ObservedLogs) AllUntimed() []LoggedEntry {
	ret := o.All()
	for i := range ret {
		ret[i].Time = time.Time{}
	}
	return ret
}

func (o *ObservedLogs) FilterLevelExact(level zapcore.Level) *ObservedLogs {
	return o.Filter(func(e LoggedEntry) bool {
		return e.Level == level
	})
}

func (o *ObservedLogs) FilterMessage(msg string) *ObservedLogs {
	return o.Filter(func(e LoggedEntry) bool {
		return e.Message == msg
	})
}

func (o *ObservedLogs) FilterMessageSnippet(snippet string) *ObservedLogs {
	return o.Filter(func(e LoggedEntry) bool {
		return strings.Contains(e.Message, snippet)
	})
}

func (o *ObservedLogs) FilterField(field zapcore.Field) *ObservedLogs {
	return o.Filter(func(e LoggedEntry) bool {
		for _, ctxField := range e.Context {
			if ctxField.Equals(field) {
				return true
			}
		}
		return false
	})
}

func (o *ObservedLogs) FilterFieldKey(key string) *ObservedLogs {
	return o.Filter(func(e LoggedEntry) bool {
		for _, ctxField := range e.Context {
			if ctxField.Key == key {
				return true
			}
		}
		return false
	})
}

// Filter returns a copy of this ObservedLogs containing only those entries
// for which the provided function returns true.
func (o *ObservedLogs) Filter(keep func(LoggedEntry) bool) *ObservedLogs {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var filtered []LoggedEntry
	for _, entry := range o.logs {
		if keep(entry) {
			filtered = append(filtered, entry)
		}
	}
	return &ObservedLogs{logs: filtered}
}

func (o *ObservedLogs) add(log LoggedEntry) {
	o.mu.Lock()
	o.logs = append(o.logs, log)
	o.mu.Unlock()
}

func New(enab zapcore.LevelEnabler) (zapcore.Core, *ObservedLogs) {
	ol := &ObservedLogs{}
	return &contextObserver{
		LevelEnabler: enab,
		logs:         ol,
	}, ol
}

type contextObserver struct {
	zapcore.LevelEnabler
	logs    *ObservedLogs
	context []zapcore.Field
}

func (co *contextObserver) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if co.Enabled(ent.Level) {
		return ce.AddCore(ent, co)
	}
	return ce
}

func (co *contextObserver) With(fields []zapcore.Field) zapcore.Core {
	return &contextObserver{
		LevelEnabler: co.LevelEnabler,
		logs:         co.logs,
		context:      append(co.context[:len(co.context):len(co.context)], fields...),
	}
}

func (co *contextObserver) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	all := make([]zapcore.Field, 0, len(fields)+len(co.context))
	all = append(all, co.context...)
	all = append(all, fields...)
	co.logs.add(LoggedEntry{ent, all})

	return nil
}

func (co *contextObserver) Sync() error {
	return nil
}
