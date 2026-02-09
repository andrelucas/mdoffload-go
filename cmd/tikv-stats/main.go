package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"math"

	"github.com/tikv/client-go/v2/txnkv"
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
		log.Fatalf("failed to create TiKV client: %v", err)
	}
	defer client.Close()

	txn, err := client.Begin()
	if err != nil {
		log.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() { _ = txn.Rollback() }()

	startKey, endKey := prefixRange([]byte(*prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		log.Fatalf("failed to create iterator: %v", err)
	}

	var keyCount int64
	var totalKeyLen int64
	var totalValueSize int64
	var maxKeyLen int
	var maxValueSize int
	minKeyLen := math.MaxInt
	minValueLen := math.MaxInt
	var keyLens []int
	var valueLens []int
	var meanKey, meanValue float64
	var m2Key, m2Value float64
	started := time.Now()

	for iter.Valid() {
		select {
		case <-ctx.Done():
			log.Fatalf("interrupted: %v", ctx.Err())
		default:
		}

		key := iter.Key()
		value := iter.Value()

		keyCount++
		klen := len(key)
		vlen := len(value)

		totalKeyLen += int64(klen)
		totalValueSize += int64(vlen)
		keyLens = append(keyLens, klen)
		valueLens = append(valueLens, vlen)

		if klen < minKeyLen {
			minKeyLen = klen
		}
		if vlen < minValueLen {
			minValueLen = vlen
		}

		if klen > maxKeyLen {
			maxKeyLen = klen
		}
		if vlen > maxValueSize {
			maxValueSize = vlen
		}

		// Welford's online algorithm for standard deviation.
		deltaKey := float64(klen) - meanKey
		meanKey += deltaKey / float64(keyCount)
		m2Key += deltaKey * (float64(klen) - meanKey)

		deltaValue := float64(vlen) - meanValue
		meanValue += deltaValue / float64(keyCount)
		m2Value += deltaValue * (float64(vlen) - meanValue)

		if err := iter.Next(); err != nil {
			log.Fatalf("iterator error: %v", err)
		}
	}

	fmt.Printf("PD addresses: %s\n", strings.Join(pdAddrs, ","))
	if *prefix == "" {
		fmt.Println("Prefix: <all keys>")
	} else {
		fmt.Printf("Prefix: %q\n", *prefix)
	}

	fmt.Printf("Keys: %d\n", keyCount)
	if keyCount == 0 {
		fmt.Println("Average key length: 0")
		fmt.Println("Max key length: 0")
		fmt.Println("Min key length: 0")
		fmt.Println("Median key length: 0")
		fmt.Println("Key length stddev: 0")
		fmt.Println("Average value size: 0")
		fmt.Println("Max value size: 0")
		fmt.Println("Min value size: 0")
		fmt.Println("Median value size: 0")
		fmt.Println("Value size stddev: 0")
	} else {
		fmt.Printf("Average key length: %.2f\n", float64(totalKeyLen)/float64(keyCount))
		fmt.Printf("Max key length: %d\n", maxKeyLen)
		fmt.Printf("Min key length: %d\n", minKeyLen)
		fmt.Printf("Median key length: %.2f\n", medianInt(keyLens))
		fmt.Printf("Key length stddev: %.2f\n", math.Sqrt(m2Key/float64(keyCount)))
		fmt.Printf("Average value size: %.2f\n", float64(totalValueSize)/float64(keyCount))
		fmt.Printf("Max value size: %d\n", maxValueSize)
		fmt.Printf("Min value size: %d\n", minValueLen)
		fmt.Printf("Median value size: %.2f\n", medianInt(valueLens))
		fmt.Printf("Value size stddev: %.2f\n", math.Sqrt(m2Value/float64(keyCount)))
	}
	fmt.Printf("Elapsed: %s\n", time.Since(started).Truncate(time.Millisecond))
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

func medianInt(values []int) float64 {
	if len(values) == 0 {
		return 0
	}

	sort.Ints(values)
	mid := len(values) / 2
	if len(values)%2 == 0 {
		return float64(values[mid-1]+values[mid]) / 2.0
	}
	return float64(values[mid])
}
