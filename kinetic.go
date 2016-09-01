package kinetic

import (
	"os"

	"github.com/Sirupsen/logrus"
	kproto "github.com/yongzhy/kinetic-go/proto"
)

// Create logger for Kinetic package
var klog = logrus.New()

func init() {
	klog.Out = os.Stdout
}

type ClientOptions struct {
	Host string
	Port int
	User int64
	Hmac []byte
}

// message type
type MessageType int32

const (
	_                     MessageType = iota
	INVALID_MESSAGE_TYPE  MessageType = iota
	GET                   MessageType = iota
	GET_RESPONSE          MessageType = iota
	PUT                   MessageType = iota
	PUT_RESPONSE          MessageType = iota
	DELETE                MessageType = iota
	DELETE_RESPONSE       MessageType = iota
	GETNEXT               MessageType = iota
	GETNEXT_RESPONSE      MessageType = iota
	GETPREVIOUS           MessageType = iota
	GETPREVIOUS_RESPONSE  MessageType = iota
	GETKEYRANGE           MessageType = iota
	GETKEYRANGE_RESPONSE  MessageType = iota
	GETVERSION            MessageType = iota
	GETVERSION_RESPONSE   MessageType = iota
	SETUP                 MessageType = iota
	SETUP_RESPONSE        MessageType = iota
	GETLOG                MessageType = iota
	GETLOG_RESPONSE       MessageType = iota
	SECURITY              MessageType = iota
	SECURITY_RESPONSE     MessageType = iota
	PEER2PEERPUSH         MessageType = iota
	PEER2PEERPUSH_RESPONS MessageType = iota
	NOOP                  MessageType = iota
	NOOP_RESPONSE         MessageType = iota
	FLUSHALLDATA          MessageType = iota
	FLUSHALLDATA_RESPONS  MessageType = iota
	PINOP                 MessageType = iota
	PINOP_RESPONSE        MessageType = iota
	MEDIASCAN             MessageType = iota
	MEDIASCAN_RESPONSE    MessageType = iota
	MEDIAOPTIMIZE         MessageType = iota
	MEDIAOPTIMIZE_RESPON  MessageType = iota
	START_BATCH           MessageType = iota
	START_BATCH_RESPONSE  MessageType = iota
	END_BATCH             MessageType = iota
	END_BATCH_RESPONSE    MessageType = iota
	ABORT_BATCH           MessageType = iota
	ABORT_BATCH_RESPONSE  MessageType = iota
)

var strMessageType = map[MessageType]string{
	INVALID_MESSAGE_TYPE:  "INVALID_MESSAGE_TYPE",
	GET:                   "GET",
	GET_RESPONSE:          "GET_RESPONSE",
	PUT:                   "PUT",
	PUT_RESPONSE:          "PUT_RESPONSE",
	DELETE:                "DELETE",
	DELETE_RESPONSE:       "DELETE_RESPONSE",
	GETNEXT:               "GETNEXT",
	GETNEXT_RESPONSE:      "GETNEXT_RESPONSE",
	GETPREVIOUS:           "GETPREVIOUS",
	GETPREVIOUS_RESPONSE:  "GETPREVIOUS_RESPONSE",
	GETKEYRANGE:           "GETKEYRANGE",
	GETKEYRANGE_RESPONSE:  "GETKEYRANGE_RESPONSE",
	GETVERSION:            "GETVERSION",
	GETVERSION_RESPONSE:   "GETVERSION_RESPONSE",
	SETUP:                 "SETUP",
	SETUP_RESPONSE:        "SETUP_RESPONSE",
	GETLOG:                "GETLOG",
	GETLOG_RESPONSE:       "GETLOG_RESPONSE",
	SECURITY:              "SECURITY",
	SECURITY_RESPONSE:     "SECURITY_RESPONSE",
	PEER2PEERPUSH:         "PEER2PEERPUSH",
	PEER2PEERPUSH_RESPONS: "PEER2PEERPUSH_RESPONS",
	NOOP:                 "NOOP",
	NOOP_RESPONSE:        "NOOP_RESPONSE",
	FLUSHALLDATA:         "FLUSHALLDATA",
	FLUSHALLDATA_RESPONS: "FLUSHALLDATA_RESPONS",
	PINOP:                "PINOP",
	PINOP_RESPONSE:       "PINOP_RESPONSE",
	MEDIASCAN:            "MEDIASCAN",
	MEDIASCAN_RESPONSE:   "MEDIASCAN_RESPONSE",
	MEDIAOPTIMIZE:        "MEDIAOPTIMIZE",
	MEDIAOPTIMIZE_RESPON: "MEDIAOPTIMIZE_RESPON",
	START_BATCH:          "START_BATCH",
	START_BATCH_RESPONSE: "START_BATCH_RESPONSE",
	END_BATCH:            "END_BATCH",
	END_BATCH_RESPONSE:   "END_BATCH_RESPONSE",
	ABORT_BATCH:          "ABORT_BATCH",
	ABORT_BATCH_RESPONSE: "ABORT_BATCH_RESPONSE",
}

func (m MessageType) String() string {
	s, ok := strMessageType[m]
	if ok {
		return s
	}
	return "Unknown MessageType"
}

func convertMessageTypeToProto(m MessageType) kproto.Command_MessageType {
	ret := kproto.Command_INVALID_MESSAGE_TYPE
	// TODO: Need add details
	return ret
}

func convertMessageTypeFromProto(m kproto.Command_MessageType) MessageType {
	var ret MessageType
	// TODO: Need add details
	return ret
}

// algorithm
type Algorithm int32

const (
	_          Algorithm = iota
	ALGO_SHA1  Algorithm = iota
	ALGO_SHA2  Algorithm = iota
	ALGO_SHA3  Algorithm = iota
	ALGO_CRC32 Algorithm = iota
	ALGO_CRC64 Algorithm = iota
)

var strAlgorithm = map[Algorithm]string{
	ALGO_SHA1:  "ALGO_SHA1",
	ALGO_SHA2:  "ALGO_SHA2",
	ALGO_SHA3:  "ALGO_SHA3",
	ALGO_CRC32: "ALGO_CRC32",
	ALGO_CRC64: "ALGO_CRC64",
}

func (a Algorithm) String() string {
	s, ok := strAlgorithm[a]
	if ok {
		return s
	}
	return "Unknown Algorithm"
}

func convertAlgoToProto(a Algorithm) kproto.Command_Algorithm {
	ret := kproto.Command_INVALID_ALGORITHM
	switch a {
	case ALGO_SHA1:
		ret = kproto.Command_SHA1
	case ALGO_SHA2:
		ret = kproto.Command_SHA2
	case ALGO_SHA3:
		ret = kproto.Command_SHA3
	case ALGO_CRC32:
		ret = kproto.Command_CRC32
	case ALGO_CRC64:
		ret = kproto.Command_CRC64
	}
	return ret
}

func convertAlgoFromProto(a kproto.Command_Algorithm) Algorithm {
	var ret Algorithm
	switch a {
	case kproto.Command_SHA1:
		ret = ALGO_SHA1
	case kproto.Command_SHA2:
		ret = ALGO_SHA2
	case kproto.Command_SHA3:
		ret = ALGO_SHA3
	case kproto.Command_CRC32:
		ret = ALGO_CRC32
	case kproto.Command_CRC64:
		ret = ALGO_CRC64
	}
	return ret
}

type Synchronization int32

const (
	_                 Synchronization = iota
	SYNC_WRITETHROUGH Synchronization = iota
	SYNC_WRITEBACK    Synchronization = iota
	SYNC_FLUSH        Synchronization = iota
)

var strSynchronization = map[Synchronization]string{
	SYNC_WRITETHROUGH: "SYNC_WRITETHROUGH",
	SYNC_WRITEBACK:    "SYNC_WRITEBACK",
	SYNC_FLUSH:        "SYNC_FLUSH",
}

func (sync Synchronization) String() string {
	s, ok := strSynchronization[sync]
	if ok {
		return s
	}
	return "Unknown Synchronization"
}

func convertSyncToProto(sync Synchronization) kproto.Command_Synchronization {
	ret := kproto.Command_INVALID_SYNCHRONIZATION
	switch sync {
	case SYNC_WRITETHROUGH:
		ret = kproto.Command_WRITETHROUGH
	case SYNC_WRITEBACK:
		ret = kproto.Command_WRITEBACK
	case SYNC_FLUSH:
		ret = kproto.Command_FLUSH
	}
	return ret
}

func convertSyncFromProto(sync kproto.Command_Synchronization) Synchronization {
	var ret Synchronization
	switch sync {
	case kproto.Command_WRITETHROUGH:
		ret = SYNC_WRITETHROUGH
	case kproto.Command_WRITEBACK:
		ret = SYNC_WRITEBACK
	case kproto.Command_FLUSH:
		ret = SYNC_FLUSH
	}
	return ret
}

type Priority int32

const (
	_                Priority = iota
	PRIORITY_LOWEST  Priority = iota
	PRIORITY_LOWER   Priority = iota
	PRIORITY_NORMAL  Priority = iota
	PRIORITY_HIGHER  Priority = iota
	PRIORITY_HIGHEST Priority = iota
)

var strPriority = map[Priority]string{
	PRIORITY_LOWEST:  "PRIORITY_LOWEST",
	PRIORITY_LOWER:   "PRIORITY_LOWER",
	PRIORITY_NORMAL:  "PRIORITY_NORMAL",
	PRIORITY_HIGHER:  "PRIORITY_HIGHER",
	PRIORITY_HIGHEST: "PRIORITY_HIGHEST",
}

func (p Priority) String() string {
	s, ok := strPriority[p]
	if ok {
		return s
	}
	return "Unknown Priority"
}

func convertPriorityToProto(p Priority) kproto.Command_Priority {
	ret := kproto.Command_NORMAL
	switch p {
	case PRIORITY_LOWEST:
		ret = kproto.Command_LOWEST
	case PRIORITY_LOWER:
		ret = kproto.Command_LOWER
	case PRIORITY_NORMAL:
		ret = kproto.Command_NORMAL
	case PRIORITY_HIGHER:
		ret = kproto.Command_HIGHER
	case PRIORITY_HIGHEST:
		ret = kproto.Command_HIGHEST
	}
	return ret
}

func convertPriorityFromProto(p kproto.Command_Priority) Priority {
	ret := PRIORITY_NORMAL
	switch p {
	case kproto.Command_LOWEST:
		ret = PRIORITY_LOWEST
	case kproto.Command_LOWER:
		ret = PRIORITY_LOWER
	case kproto.Command_NORMAL:
		ret = PRIORITY_NORMAL
	case kproto.Command_HIGHER:
		ret = PRIORITY_HIGHER
	case kproto.Command_HIGHEST:
		ret = PRIORITY_HIGHEST
	}
	return ret
}

type Permission int32

const (
	_                   Permission = iota
	PERMISSION_READ     Permission = iota
	PERMISSION_WRITE    Permission = iota
	PERMISSION_DELETE   Permission = iota
	PERMISSION_RANGE    Permission = iota
	PERMISSION_SETUP    Permission = iota
	PERMISSION_P2POP    Permission = iota
	PERMISSION_GETLOG   Permission = iota
	PERMISSION_SECURITY Permission = iota
)

var strPermission = map[Permission]string{
	PERMISSION_READ:     "PERMISSION_READ",
	PERMISSION_WRITE:    "PERMISSION_WRITE",
	PERMISSION_DELETE:   "PERMISSION_DELETE",
	PERMISSION_RANGE:    "PERMISSION_RANGE",
	PERMISSION_SETUP:    "PERMISSION_SETUP",
	PERMISSION_P2POP:    "PERMISSION_P2POP",
	PERMISSION_GETLOG:   "PERMISSION_GETLOG",
	PERMISSION_SECURITY: "PERMISSION_SECURITY",
}

func (p Permission) String() string {
	s, ok := strPermission[p]
	if ok {
		return s
	}
	return "Unknown Permission"
}

type Record struct {
	Key      []byte
	Value    []byte
	Version  []byte
	Tag      []byte
	Algo     Algorithm
	Sync     Synchronization
	Force    bool
	MetaOnly bool
}

type KeyRange struct {
	StartKey          []byte
	EndKey            []byte
	StartKeyInclusive bool
	EndKeyInclusive   bool
	Reverse           bool
	Max               int32
}
