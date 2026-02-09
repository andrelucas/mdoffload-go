package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	stdlog "log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	plog "github.com/pingcap/log"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	// keep stdout clean for JSON; send diagnostics to stderr explicitly
	stdlog.SetOutput(os.Stderr)
	configureTiKVLogging()

	var pdAddrs stringSlice
	flag.Var(&pdAddrs, "pd", "TiKV PD address (repeatable)")
	flag.Var(&pdAddrs, "u", "shorthand for --pd")
	prefix := flag.String("prefix", "", "optional key prefix to limit the scan")
	flag.Parse()

	if len(pdAddrs) == 0 {
		pdAddrs = append(pdAddrs, "http://127.0.0.1:2379")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		stdlog.Fatalf("failed to create TiKV client: %v", err)
	}
	defer client.Close()

	txn, err := client.Begin()
	if err != nil {
		stdlog.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() { _ = txn.Rollback() }()

	startKey, endKey := prefixRange([]byte(*prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		stdlog.Fatalf("failed to create iterator: %v", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	writeJSONStream(iter, encoder, ctx)
}

// configureTiKVLogging forces TiKV client's zap logger to stderr only.
func configureTiKVLogging() {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.OutputPaths = []string{"stderr"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	cfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)

	logger, err := cfg.Build()
	if err != nil {
		stdlog.Fatalf("failed to build zap logger: %v", err)
	}
	props := &plog.ZapProperties{
		Core:   logger.Core(),
		Syncer: zapcore.AddSync(os.Stderr),
		Level:  cfg.Level,
	}
	plog.ReplaceGlobals(logger, props)
}

// kvIterator is the minimal iterator contract we need; keeps us independent of concrete client-go iterator types.
type kvIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
}

// writeJSONStream emits a JSON array of {"key":"...","value":"..."} objects using base64 for binary safety.
func writeJSONStream(iter kvIterator, encoder *json.Encoder, ctx context.Context) {
	if _, err := os.Stdout.WriteString("{\"entries\": ["); err != nil {
		stdlog.Fatalf("failed to write output: %v", err)
	}

	first := true
	start := time.Now()

	for iter.Valid() {
		select {
		case <-ctx.Done():
			stdlog.Fatalf("interrupted: %v", ctx.Err())
		default:
		}

		entry := struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{
			Key:   base64.StdEncoding.EncodeToString(iter.Key()),
			Value: base64.StdEncoding.EncodeToString(iter.Value()),
		}

		if !first {
			if _, err := os.Stdout.WriteString(","); err != nil {
				stdlog.Fatalf("failed to write output: %v", err)
			}
		}

		if err := encoder.Encode(entry); err != nil {
			stdlog.Fatalf("failed to encode entry: %v", err)
		}

		first = false

		if err := iter.Next(); err != nil {
			stdlog.Fatalf("iterator error: %v", err)
		}
	}

	if _, err := os.Stdout.WriteString("]}\n"); err != nil {
		stdlog.Fatalf("failed to write output: %v", err)
	}

	_ = start // reserved for potential future logging
}

// prefixRange builds the start/end keys needed to iterate a prefix without pulling unrelated keys.
func prefixRange(prefix []byte) ([]byte, []byte) {
	if len(prefix) == 0 {
		return []byte{}, []byte{0xFF}
	}

	end := make([]byte, len(prefix)+1)
	copy(end, prefix)
	end[len(prefix)] = 0xFF
	return prefix, end
}
