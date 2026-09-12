//go:build observe
// +build observe

package observe

import (
	"testing"
	"time"
)

func TestLogLineTimeParsesZapJSONTimestamp(t *testing.T) {
	got, ok := logLineTime(`{"level":"info","ts":"2026-09-11T16:13:33.126+0800","msg":"registered"}`)
	if !ok {
		t.Fatal("logLineTime() did not parse zap JSON ts")
	}
	want := time.Date(2026, time.September, 11, 16, 13, 33, 126000000, time.FixedZone("+0800", 8*60*60))
	if !got.Equal(want) {
		t.Fatalf("logLineTime() = %s, want %s", got, want)
	}
}

func TestLogLineTimeParsesPlainAndPrefixedTimestamp(t *testing.T) {
	for _, line := range []string{
		"2026-09-11 16:13:33.126 registered instance",
		"INFO 2026-09-11T16:13:33.126+08:00 registered instance",
	} {
		if got, ok := logLineTime(line); !ok || got.IsZero() {
			t.Fatalf("logLineTime(%q) = %s, %v; want timestamp", line, got, ok)
		}
	}
}

func TestLogLineTimeParsesNumericZapTimestamp(t *testing.T) {
	got, ok := logLineTime(`{"ts":1789136018.281,"msg":"registered"}`)
	if !ok {
		t.Fatal("logLineTime() did not parse numeric zap ts")
	}
	if got.Unix() != 1789136018 || got.Nanosecond() != 281000000 {
		t.Fatalf("numeric timestamp = %s, want seconds/nanos 1789136018/281000000", got)
	}
}
