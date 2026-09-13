package notice

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	legacylog "spotter/pkg/log"
	"testing"
)

func TestLegacyNoticeInitPreservesAppCodeAndEnvironment(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	oldLogger, oldNoticer := legacylog.Logger, Noticer
	t.Cleanup(func() { legacylog.Logger, Noticer = oldLogger, oldNoticer })
	legacylog.Logger = zap.New(core).Sugar()
	InitNoticeClient("test")
	if Noticer == nil {
		t.Fatal("InitNoticeClient did not create a notifier")
	}
	if err := Noticer.SendNotice("title", "content", "emergency", "text"); err != nil {
		t.Fatalf("SendNotice: %v", err)
	}
	if logs.Len() != 1 || logs.All()[0].Message != "[notice] appcode: spotter-mtech, env: test, level: emergency, type: text, title: title, content: content" {
		t.Fatalf("unexpected local mirror: %+v", logs.All())
	}
}
