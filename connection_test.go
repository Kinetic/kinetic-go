package kinetic

import (
	"os"
	"testing"
)

var (
	blockConn    *BlockConnection    = nil
	nonblockConn *NonBlockConnection = nil
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

func TestBlockNoOp(t *testing.T) {
	status, err := blockConn.NoOp()
	if err != nil || status.Code != OK {
		t.Fatal("Blocking NoOp Failure", err, status.String())
	}
}

func TestBlockGet(t *testing.T) {
	_, status, err := blockConn.Get([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking Get Failure", err, status.String())
	}
}

func TestBlockGetNext(t *testing.T) {
	_, status, err := blockConn.GetNext([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking GetNext Failure", err, status.String())
	}
}

func TestBlockGetPrevious(t *testing.T) {
	_, status, err := blockConn.GetPrevious([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking GetPrevious Failure", err, status.String())
	}
}

func TestBlockGetVersion(t *testing.T) {
	version, status, err := blockConn.GetVersion([]byte("object000"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking GetVersion Failure", err, status.String())
	}
	t.Logf("Object version = %x", version)
}

func TestBlockFlush(t *testing.T) {
	status, err := blockConn.Flush()
	if err != nil || status.Code != OK {
		t.Fatal("Blocking Flush Failure", err, status.String())
	}
}

func TestBlockPut(t *testing.T) {
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
		t.Fatal("Blocking Put Failure", err, status.String())
	}
}

func TestBlockDelete(t *testing.T) {
	entry := Record{
		Key:   []byte("object001"),
		Sync:  SYNC_WRITETHROUGH,
		Algo:  ALGO_SHA1,
		Force: true,
	}
	status, err := blockConn.Delete(&entry)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking Delete Failure", err, status.String())
	}
}

func TestBlockGetKeyRange(t *testing.T) {
	r := KeyRange{
		StartKey:          []byte("object000"),
		EndKey:            []byte("object999"),
		StartKeyInclusive: true,
		EndKeyInclusive:   true,
		Max:               5,
	}
	keys, status, err := blockConn.GetKeyRange(&r)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking GetKeyRange Failure: ", status.Error())
	}
	for k, key := range keys {
		t.Logf("key[%d] = %s", k, string(key))
	}
}

func TestBlockGetLogCapacity(t *testing.T) {
	logs := []LogType{
		LOG_CAPACITIES,
	}
	klogs, status, err := blockConn.GetLog(logs)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking GetLog Failure", err, status.String())
	}
	if !(klogs.Capacity.CapacityInBytes > 0 &&
		klogs.Capacity.PortionFull > 0) {
		t.Logf("%#v", klogs.Capacity)
		t.Fatal("Blocking GetLog for Capacity Failure", err, status.String())
	}
}

func TestBlockGetLogLimit(t *testing.T) {
	logs := []LogType{
		LOG_LIMITS,
	}
	klogs, status, err := blockConn.GetLog(logs)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking GetLog Failure", err, status.String())
	}
	if klogs.Limits.MaxKeySize != 4096 ||
		klogs.Limits.MaxValueSize != 1024*1024 {
		t.Logf("%#v", klogs.Limits)
		t.Fatal("Blocking GetLog for Limits Failure", err, status.String())
	}
}

func TestBlockGetLogAll(t *testing.T) {
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
		t.Fatal("Blocking GetLog Failure", err, status.String())
	}
	if klogs.Limits.MaxKeySize != 4096 ||
		klogs.Limits.MaxValueSize != 1024*1024 {
		t.Logf("%#v", klogs.Limits)
		t.Fatal("Blocking GetLog, Limits Failure", err, status.String())
	}
	if !(klogs.Capacity.CapacityInBytes > 0 &&
		klogs.Capacity.PortionFull > 0) {
		t.Logf("%#v", klogs.Capacity)
		t.Fatal("Blocking GetLog, Capacity Failure", err, status.String())
	}
}

func TestBlockMediaScan(t *testing.T) {
	op := MediaOperation{
		StartKey:          []byte("object000"),
		EndKey:            []byte("object999"),
		StartKeyInclusive: true,
		EndKeyInclusive:   true,
	}
	status, err := blockConn.MediaScan(&op, PRIORITY_NORMAL)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking MediaScan Failure: ", err, status.String())
	}
}

func TestBlockMediaOptimize(t *testing.T) {
	op := MediaOperation{
		StartKey:          []byte("object000"),
		EndKey:            []byte("object999"),
		StartKeyInclusive: true,
		EndKeyInclusive:   true,
	}
	status, err := blockConn.MediaOptimize(&op, PRIORITY_NORMAL)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking MediaOptimize Failure: ", err, status.String())
	}
}
