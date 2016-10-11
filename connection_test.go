/**
 * Copyright 2013-2016 Seagate Technology LLC.
 *
 * This Source Code Form is subject to the terms of the Mozilla
 * Public License, v. 2.0. If a copy of the MPL was not
 * distributed with this file, You can obtain one at
 * https://mozilla.org/MP:/2.0/.
 *
 * This program is distributed in the hope that it will be useful,
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public
 * License for more details.
 *
 * See www.openkinetic.org for more project information
 */

package kinetic

import (
	"bytes"
	"os"
	"testing"
)

var (
	blockConn    *BlockConnection
	nonblockConn *NonBlockConnection
)

var option = ClientOptions{
	Host: "127.0.0.1",
	Port: 8123,
	//Port:   8443, // For SSL connection
	User: 1,
	Hmac: []byte("asdfasdf"),
	//UseSSL: true,
}

func TestMain(m *testing.M) {
	SetLogLevel(LogLevelDebug)
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
		Key:   []byte("object000"),
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

// TestBlockPut_keyOverflow test key buffer length than MaxKeySize
// TODO: drive implementation using UNSOLICITEDSTATUS for Key too long.
func TestBlockPut_keyOverflow(t *testing.T) {
	entry := Record{
		Key:   bytes.Repeat([]byte("K"), int(blockConn.nbc.service.device.Limits.MaxKeySize+1)),
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

// TestBlockPut_valueOverflow test key buffer length than MaxValueSize
// TODO: drive implementation using UNSOLICITEDSTATUS.
func TestBlockPut_valueOverflow(t *testing.T) {
	entry := Record{
		Key:   []byte("key"),
		Value: bytes.Repeat([]byte("V"), int(blockConn.nbc.service.device.Limits.MaxValueSize+1)),
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

// TestBlockPut_tagOverflow test key buffer length than MaxTagSize
// TODO: drive implementation using UNSOLICITEDSTATUS.
func TestBlockPut_tagOverflow(t *testing.T) {
	entry := Record{
		Key:   []byte("key"),
		Value: []byte("value"),
		Sync:  SYNC_WRITETHROUGH,
		Algo:  ALGO_SHA1,
		Tag:   bytes.Repeat([]byte("T"), int(blockConn.nbc.service.device.Limits.MaxTagSize+1)),
		Force: true,
	}
	status, err := blockConn.Put(&entry)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking Put Failure", err, status.String())
	}
}

func TestBlockDelete(t *testing.T) {
	entry := Record{
		Key:   []byte("object000"),
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

func TestBlockSetClusterVersion(t *testing.T) {
	status, err := blockConn.SetClusterVersion(1)
	if err != nil || status.Code != OK {
		t.Fatal("Blocking SetClusterVersion Failure: ", err, status.String())
	}

	blockConn.SetClientClusterVersion(2)
	_, status, err = blockConn.Get([]byte("object000"))
	if err != nil || status.Code != REMOTE_CLUSTER_VERSION_MISMATCH {
		t.Fatal("Blocking Get expected REMOTE_CLUSTER_VERSION_MISMATCH. ", err, status.String())
	}
	t.Log(status.String())
}

func TestBlockInstantErase(t *testing.T) {
	t.Skip("Danger: Skip InstanceErase Test")
	status, err := blockConn.InstantErase([]byte("PIN"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking InstantErase Failure: ", err, status.String())
	}
}

func TestBlockSecureErase(t *testing.T) {
	t.Skip("Danger: Skip SecureErase Test")
	status, err := blockConn.SecureErase([]byte(""))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking SecureErase Failure: ", err, status.String())
	}
}

func TestBlockSetErasePin(t *testing.T) {
	t.Skip("Danger: Skip SetErasePin Test")
	status, err := blockConn.SetErasePin([]byte(""), []byte("PIN"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking SetErasePin Failure: ", err, status.String())
	}
}

func TestBlockSetLockPin(t *testing.T) {
	t.Skip("Danger: Skip SetLockPin Test")
	status, err := blockConn.SetLockPin([]byte(""), []byte("PIN"))
	if err != nil || status.Code != OK {
		t.Fatal("Blocking SetLockPin Failure: ", err, status.String())
	}
}
