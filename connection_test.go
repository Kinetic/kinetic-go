package kinetic

import (
	"os"
	"testing"
)

var (
	blockConn *BlockConnection = nil
)

var option = ClientOptions{
	Host: "10.29.24.55",
	Port: 8123,
	User: 1,
	Hmac: []byte("asdfasdf")}

func TestMain(m *testing.M) {
	blockConn, _ = NewBlockConnection(option)
	if blockConn != nil {
		code := m.Run()
		blockConn.Close()
		os.Exit(code)
	} else {
		os.Exit(-1)
	}
}

func TestNonBlockNoOp(t *testing.T) {
	status, err := blockConn.NoOp()
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking NoOp Failure")
	}
}

func TestNonBlockGet(t *testing.T) {
	_, status, err := blockConn.Get([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking Get Failure")
	}
}

func TestNonBlockGetNext(t *testing.T) {
	_, status, err := blockConn.GetNext([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetNext Failure")
	}
}

func TestNonBlockGetPrevious(t *testing.T) {
	_, status, err := blockConn.GetPrevious([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetPrevious Failure")
	}
}

func TestNonBlockGetVersion(t *testing.T) {
	version, status, err := blockConn.GetVersion([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetVersion Failure")
	}
	t.Logf("Object version = %x", version)
}

func TestNonBlockFlush(t *testing.T) {
	status, err := blockConn.Flush()
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking Flush Failure")
	}
}

func TestNonBlockPut(t *testing.T) {
	entry := Record{
		Key:   []byte("object001"),
		Value: []byte("ABCDEFG"),
		Sync:  SYNC_WRITETHROUGH,
		Algo:  ALGO_SHA1,
		Tag:   []byte(""),
		Force: true,
	}
	status, err := blockConn.Put(&entry)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking Put Failure")
	}
}

func TestNonBlockDelete(t *testing.T) {
	entry := Record{
		Key:   []byte("object001"),
		Sync:  SYNC_WRITETHROUGH,
		Algo:  ALGO_SHA1,
		Force: true,
	}
	status, err := blockConn.Delete(&entry)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking Delete Failure")
	}
}

func TestNonBlockGetKeyRange(t *testing.T) {
	r := KeyRange{
		StartKey:          []byte("object000"),
		EndKey:            []byte("object999"),
		StartKeyInclusive: true,
		EndKeyInclusive:   true,
		Max:               5,
	}
	keys, status, err := blockConn.GetKeyRange(&r)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetKeyRange Failure: ", status.Error())
	}
	for k, key := range keys {
		t.Logf("key[%d] = %s", k, string(key))
	}
}

func TestNonBlockGetLogCapacity(t *testing.T) {
	logs := []LogType{
		LOG_CAPACITIES,
	}
	klogs, status, err := blockConn.GetLog(logs)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetLog Failure")
	}
	if !(klogs.Capacity.CapacityInBytes > 0 &&
		klogs.Capacity.PortionFull > 0) {
		t.Logf("%#v", klogs.Capacity)
		t.Fatal("Nonblocking GetLog for Capacity Failure")
	}
}

func TestNonBlockGetLogLimit(t *testing.T) {
	logs := []LogType{
		LOG_LIMITS,
	}
	klogs, status, err := blockConn.GetLog(logs)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetLog Failure")
	}
	if klogs.Limits.MaxKeySize != 4096 ||
		klogs.Limits.MaxValueSize != 1024*1024 {
		t.Logf("%#v", klogs.Limits)
		t.Fatal("Nonblocking GetLog for Limits Failure")
	}
}

func TestNonBlockGetLogAll(t *testing.T) {
	logs := []LogType{
		LOG_UTILIZATIONS,
		LOG_TEMPERATURES,
		LOG_CAPACITIES,
		LOG_CONFIGURATION,
		LOG_STATISTICS,
		LOG_MESSAGES,
		LOG_LIMITS,
	}
	klogs, status, err := blockConn.GetLog(logs)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking GetLog Failure")
	}
	t.Logf("GetLog %+v", klogs)
}

func TestNonBlockMediaScan(t *testing.T) {
	op := MediaOperation{
		StartKey:          []byte("object000"),
		EndKey:            []byte("object999"),
		StartKeyInclusive: true,
		EndKeyInclusive:   true,
	}
	status, err := blockConn.MediaScan(&op, PRIORITY_NORMAL)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking MediaScan Failure: ", status.Error())
	}
}

func TestNonBlockMediaOptimize(t *testing.T) {
	op := MediaOperation{
		StartKey:          []byte("object000"),
		EndKey:            []byte("object999"),
		StartKeyInclusive: true,
		EndKeyInclusive:   true,
	}
	status, err := blockConn.MediaOptimize(&op, PRIORITY_NORMAL)
	if err != nil || status.Code != OK {
		t.Fatal("Nonblocking MediaOptimize Failure: ", status.Error())
	}
}
