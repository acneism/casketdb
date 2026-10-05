package bitcask

import (
	"fmt"
	"strings"
	"time"
)

type SyncPolicy int

const (
	SyncEverySec SyncPolicy = iota
	SyncAlways
	SyncNo
)

const noSyncInterval = 30

func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncNo:
		return "no"
	default:
		return "everysec"
	}
}

func ParseSyncPolicy(s string) (SyncPolicy, error) {
	switch strings.ToLower(s) {
	case "always":
		return SyncAlways, nil
	case "everysec":
		return SyncEverySec, nil
	case "no":
		return SyncNo, nil
	}
	return 0, fmt.Errorf("bitcask: unknown sync policy %q", s)
}

type Options struct {
	MaxFileSize    int64
	Sync           SyncPolicy
	MergeRatio     float64
	MergeMinBytes  int64
	MergeInterval  time.Duration
	ExpireInterval time.Duration
	Logs           int
	Now            func() time.Time
}

func DefaultOptions() Options {
	return Options{
		MaxFileSize:    64 << 20,
		Sync:           SyncEverySec,
		MergeRatio:     0.5,
		MergeMinBytes:  64 << 20,
		MergeInterval:  time.Minute,
		ExpireInterval: 100 * time.Millisecond,
		Now:            time.Now,
	}
}
