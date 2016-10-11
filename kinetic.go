/*

Package kinetic is golang kinetic client implementation.

For details about kinetic protocol, please refer to https://github.com/Kinetic/kinetic-protocol

*/
package kinetic

import (
	"io"
	"os"

	"github.com/Sirupsen/logrus"
	kproto "github.com/yongzhy/kinetic-go/proto"
)

// Create logger for Kinetic package
var klog = logrus.New()

func init() {
	klog.Out = os.Stdout
	klog.Level = logrus.InfoLevel
}

// LogLevel defines the logging level for kinetic Go library. Default is LogLevelInfo.
type LogLevel logrus.Level

const (
	LogLevelPanic LogLevel = LogLevel(logrus.PanicLevel)
	// LogLevelFatal level. Logs and then calls `os.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	LogLevelFatal LogLevel = LogLevel(logrus.FatalLevel)
	// LogLevelError level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	LogLevelError LogLevel = LogLevel(logrus.ErrorLevel)
	// LogLevelWarn level. Non-critical entries that deserve eyes.
	LogLevelWarn LogLevel = LogLevel(logrus.WarnLevel)
	// LogLevelInfo level. General operational entries about what's going on inside the
	// application.
	LogLevelInfo LogLevel = LogLevel(logrus.InfoLevel)
	// LogLevelDebug level. Usually only enabled when debugging. Very verbose logging.
	LogLevelDebug LogLevel = LogLevel(logrus.DebugLevel)
)

// SetLogLevel sets kinetic libary log level
func SetLogLevel(l LogLevel) {
	klog.Level = logrus.Level(l)
}

// SetLogOutput sets kinetic libary log output
func SetLogOutput(out io.Writer) {
	klog.Out = out
}

// ClientOptions specify connection options to kinetic device.
type ClientOptions struct {
	Host   string // Kinetic device IP address
	Port   int    // Network port to connect, if UseSSL is true, this port should be the TlsPort
	User   int64  // User Id
	Hmac   []byte
	UseSSL bool // Use SSL connection, or plain connection
}

// MessageType are the top level kinetic command message type.
type MessageType int32

const (
	_                             MessageType = iota
	MESSAGE_GET                   MessageType = iota
	MESSAGE_GET_RESPONSE          MessageType = iota
	MESSAGE_PUT                   MessageType = iota
	MESSAGE_PUT_RESPONSE          MessageType = iota
	MESSAGE_DELETE                MessageType = iota
	MESSAGE_DELETE_RESPONSE       MessageType = iota
	MESSAGE_GETNEXT               MessageType = iota
	MESSAGE_GETNEXT_RESPONSE      MessageType = iota
	MESSAGE_GETPREVIOUS           MessageType = iota
	MESSAGE_GETPREVIOUS_RESPONSE  MessageType = iota
	MESSAGE_GETKEYRANGE           MessageType = iota
	MESSAGE_GETKEYRANGE_RESPONSE  MessageType = iota
	MESSAGE_GETVERSION            MessageType = iota
	MESSAGE_GETVERSION_RESPONSE   MessageType = iota
	MESSAGE_SETUP                 MessageType = iota
	MESSAGE_SETUP_RESPONSE        MessageType = iota
	MESSAGE_GETLOG                MessageType = iota
	MESSAGE_GETLOG_RESPONSE       MessageType = iota
	MESSAGE_SECURITY              MessageType = iota
	MESSAGE_SECURITY_RESPONSE     MessageType = iota
	MESSAGE_PEER2PEERPUSH         MessageType = iota
	MESSAGE_PEER2PEERPUSH_RESPONS MessageType = iota
	MESSAGE_NOOP                  MessageType = iota
	MESSAGE_NOOP_RESPONSE         MessageType = iota
	MESSAGE_FLUSHALLDATA          MessageType = iota
	MESSAGE_FLUSHALLDATA_RESPONS  MessageType = iota
	MESSAGE_PINOP                 MessageType = iota
	MESSAGE_PINOP_RESPONSE        MessageType = iota
	MESSAGE_MEDIASCAN             MessageType = iota
	MESSAGE_MEDIASCAN_RESPONSE    MessageType = iota
	MESSAGE_MEDIAOPTIMIZE         MessageType = iota
	MESSAGE_MEDIAOPTIMIZE_RESPON  MessageType = iota
	MESSAGE_START_BATCH           MessageType = iota
	MESSAGE_START_BATCH_RESPONSE  MessageType = iota
	MESSAGE_END_BATCH             MessageType = iota
	MESSAGE_END_BATCH_RESPONSE    MessageType = iota
	MESSAGE_ABORT_BATCH           MessageType = iota
	MESSAGE_ABORT_BATCH_RESPONSE  MessageType = iota
)

var strMessageType = map[MessageType]string{
	MESSAGE_GET:                   "GET",
	MESSAGE_GET_RESPONSE:          "GET_RESPONSE",
	MESSAGE_PUT:                   "PUT",
	MESSAGE_PUT_RESPONSE:          "PUT_RESPONSE",
	MESSAGE_DELETE:                "DELETE",
	MESSAGE_DELETE_RESPONSE:       "DELETE_RESPONSE",
	MESSAGE_GETNEXT:               "GETNEXT",
	MESSAGE_GETNEXT_RESPONSE:      "GETNEXT_RESPONSE",
	MESSAGE_GETPREVIOUS:           "GETPREVIOUS",
	MESSAGE_GETPREVIOUS_RESPONSE:  "GETPREVIOUS_RESPONSE",
	MESSAGE_GETKEYRANGE:           "GETKEYRANGE",
	MESSAGE_GETKEYRANGE_RESPONSE:  "GETKEYRANGE_RESPONSE",
	MESSAGE_GETVERSION:            "GETVERSION",
	MESSAGE_GETVERSION_RESPONSE:   "GETVERSION_RESPONSE",
	MESSAGE_SETUP:                 "SETUP",
	MESSAGE_SETUP_RESPONSE:        "SETUP_RESPONSE",
	MESSAGE_GETLOG:                "GETLOG",
	MESSAGE_GETLOG_RESPONSE:       "GETLOG_RESPONSE",
	MESSAGE_SECURITY:              "SECURITY",
	MESSAGE_SECURITY_RESPONSE:     "SECURITY_RESPONSE",
	MESSAGE_PEER2PEERPUSH:         "PEER2PEERPUSH",
	MESSAGE_PEER2PEERPUSH_RESPONS: "PEER2PEERPUSH_RESPONS",
	MESSAGE_NOOP:                  "NOOP",
	MESSAGE_NOOP_RESPONSE:         "NOOP_RESPONSE",
	MESSAGE_FLUSHALLDATA:          "FLUSHALLDATA",
	MESSAGE_FLUSHALLDATA_RESPONS:  "FLUSHALLDATA_RESPONS",
	MESSAGE_PINOP:                 "PINOP",
	MESSAGE_PINOP_RESPONSE:        "PINOP_RESPONSE",
	MESSAGE_MEDIASCAN:             "MEDIASCAN",
	MESSAGE_MEDIASCAN_RESPONSE:    "MEDIASCAN_RESPONSE",
	MESSAGE_MEDIAOPTIMIZE:         "MEDIAOPTIMIZE",
	MESSAGE_MEDIAOPTIMIZE_RESPON:  "MEDIAOPTIMIZE_RESPON",
	MESSAGE_START_BATCH:           "START_BATCH",
	MESSAGE_START_BATCH_RESPONSE:  "START_BATCH_RESPONSE",
	MESSAGE_END_BATCH:             "END_BATCH",
	MESSAGE_END_BATCH_RESPONSE:    "END_BATCH_RESPONSE",
	MESSAGE_ABORT_BATCH:           "ABORT_BATCH",
	MESSAGE_ABORT_BATCH_RESPONSE:  "ABORT_BATCH_RESPONSE",
}

func (m MessageType) String() string {
	str, ok := strMessageType[m]
	if ok {
		return str
	}
	return "Unknown MessageType"
}

func convertMessageTypeToProto(m MessageType) kproto.Command_MessageType {
	ret := kproto.Command_INVALID_MESSAGE_TYPE
	switch m {
	case MESSAGE_GET:
		ret = kproto.Command_GET
	case MESSAGE_GET_RESPONSE:
		ret = kproto.Command_GET_RESPONSE
	case MESSAGE_PUT:
		ret = kproto.Command_PUT
	case MESSAGE_PUT_RESPONSE:
		ret = kproto.Command_PUT_RESPONSE
	case MESSAGE_DELETE:
		ret = kproto.Command_DELETE
	case MESSAGE_DELETE_RESPONSE:
		ret = kproto.Command_DELETE_RESPONSE
	case MESSAGE_GETNEXT:
		ret = kproto.Command_GETNEXT
	case MESSAGE_GETNEXT_RESPONSE:
		ret = kproto.Command_GETNEXT_RESPONSE
	case MESSAGE_GETPREVIOUS:
		ret = kproto.Command_GETPREVIOUS
	case MESSAGE_GETPREVIOUS_RESPONSE:
		ret = kproto.Command_GETPREVIOUS_RESPONSE
	case MESSAGE_GETKEYRANGE:
		ret = kproto.Command_GETKEYRANGE
	case MESSAGE_GETKEYRANGE_RESPONSE:
		ret = kproto.Command_GETKEYRANGE_RESPONSE
	case MESSAGE_GETVERSION:
		ret = kproto.Command_GETVERSION
	case MESSAGE_GETVERSION_RESPONSE:
		ret = kproto.Command_GETVERSION_RESPONSE
	case MESSAGE_SETUP:
		ret = kproto.Command_SETUP
	case MESSAGE_SETUP_RESPONSE:
		ret = kproto.Command_SETUP_RESPONSE
	case MESSAGE_GETLOG:
		ret = kproto.Command_GETLOG
	case MESSAGE_GETLOG_RESPONSE:
		ret = kproto.Command_GETLOG_RESPONSE
	case MESSAGE_SECURITY:
		ret = kproto.Command_SECURITY
	case MESSAGE_SECURITY_RESPONSE:
		ret = kproto.Command_SECURITY_RESPONSE
	case MESSAGE_PEER2PEERPUSH:
		ret = kproto.Command_PEER2PEERPUSH
	case MESSAGE_PEER2PEERPUSH_RESPONS:
		ret = kproto.Command_PEER2PEERPUSH_RESPONSE
	case MESSAGE_NOOP:
		ret = kproto.Command_NOOP
	case MESSAGE_NOOP_RESPONSE:
		ret = kproto.Command_NOOP_RESPONSE
	case MESSAGE_FLUSHALLDATA:
		ret = kproto.Command_FLUSHALLDATA
	case MESSAGE_FLUSHALLDATA_RESPONS:
		ret = kproto.Command_FLUSHALLDATA_RESPONSE
	case MESSAGE_PINOP:
		ret = kproto.Command_PINOP
	case MESSAGE_PINOP_RESPONSE:
		ret = kproto.Command_PINOP_RESPONSE
	case MESSAGE_MEDIASCAN:
		ret = kproto.Command_MEDIASCAN
	case MESSAGE_MEDIASCAN_RESPONSE:
		ret = kproto.Command_MEDIASCAN_RESPONSE
	case MESSAGE_MEDIAOPTIMIZE:
		ret = kproto.Command_MEDIAOPTIMIZE
	case MESSAGE_MEDIAOPTIMIZE_RESPON:
		ret = kproto.Command_MEDIAOPTIMIZE_RESPONSE
	case MESSAGE_START_BATCH:
		ret = kproto.Command_START_BATCH
	case MESSAGE_START_BATCH_RESPONSE:
		ret = kproto.Command_START_BATCH_RESPONSE
	case MESSAGE_END_BATCH:
		ret = kproto.Command_END_BATCH
	case MESSAGE_END_BATCH_RESPONSE:
		ret = kproto.Command_END_BATCH_RESPONSE
	case MESSAGE_ABORT_BATCH:
		ret = kproto.Command_ABORT_BATCH
	case MESSAGE_ABORT_BATCH_RESPONSE:
		ret = kproto.Command_ABORT_BATCH_RESPONSE
	}
	return ret
}

func convertMessageTypeFromProto(m kproto.Command_MessageType) MessageType {
	var ret MessageType
	switch m {
	case kproto.Command_GET:
		ret = MESSAGE_GET
	case kproto.Command_GET_RESPONSE:
		ret = MESSAGE_GET_RESPONSE
	case kproto.Command_PUT:
		ret = MESSAGE_PUT
	case kproto.Command_PUT_RESPONSE:
		ret = MESSAGE_PUT_RESPONSE
	case kproto.Command_DELETE:
		ret = MESSAGE_DELETE
	case kproto.Command_DELETE_RESPONSE:
		ret = MESSAGE_DELETE_RESPONSE
	case kproto.Command_GETNEXT:
		ret = MESSAGE_GETNEXT
	case kproto.Command_GETNEXT_RESPONSE:
		ret = MESSAGE_GETNEXT_RESPONSE
	case kproto.Command_GETPREVIOUS:
		ret = MESSAGE_GETPREVIOUS
	case kproto.Command_GETPREVIOUS_RESPONSE:
		ret = MESSAGE_GETPREVIOUS_RESPONSE
	case kproto.Command_GETKEYRANGE:
		ret = MESSAGE_GETKEYRANGE
	case kproto.Command_GETKEYRANGE_RESPONSE:
		ret = MESSAGE_GETKEYRANGE_RESPONSE
	case kproto.Command_GETVERSION:
		ret = MESSAGE_GETVERSION
	case kproto.Command_GETVERSION_RESPONSE:
		ret = MESSAGE_GETVERSION_RESPONSE
	case kproto.Command_SETUP:
		ret = MESSAGE_SETUP
	case kproto.Command_SETUP_RESPONSE:
		ret = MESSAGE_SETUP_RESPONSE
	case kproto.Command_GETLOG:
		ret = MESSAGE_GETLOG
	case kproto.Command_GETLOG_RESPONSE:
		ret = MESSAGE_GETLOG_RESPONSE
	case kproto.Command_SECURITY:
		ret = MESSAGE_SECURITY
	case kproto.Command_SECURITY_RESPONSE:
		ret = MESSAGE_SECURITY_RESPONSE
	case kproto.Command_PEER2PEERPUSH:
		ret = MESSAGE_PEER2PEERPUSH
	case kproto.Command_PEER2PEERPUSH_RESPONSE:
		ret = MESSAGE_PEER2PEERPUSH_RESPONS
	case kproto.Command_NOOP:
		ret = MESSAGE_NOOP
	case kproto.Command_NOOP_RESPONSE:
		ret = MESSAGE_NOOP_RESPONSE
	case kproto.Command_FLUSHALLDATA:
		ret = MESSAGE_FLUSHALLDATA
	case kproto.Command_FLUSHALLDATA_RESPONSE:
		ret = MESSAGE_FLUSHALLDATA_RESPONS
	case kproto.Command_PINOP:
		ret = MESSAGE_PINOP
	case kproto.Command_PINOP_RESPONSE:
		ret = MESSAGE_PINOP_RESPONSE
	case kproto.Command_MEDIASCAN:
		ret = MESSAGE_MEDIASCAN
	case kproto.Command_MEDIASCAN_RESPONSE:
		ret = MESSAGE_MEDIASCAN_RESPONSE
	case kproto.Command_MEDIAOPTIMIZE:
		ret = MESSAGE_MEDIAOPTIMIZE
	case kproto.Command_MEDIAOPTIMIZE_RESPONSE:
		ret = MESSAGE_MEDIAOPTIMIZE_RESPON
	case kproto.Command_START_BATCH:
		ret = MESSAGE_START_BATCH
	case kproto.Command_START_BATCH_RESPONSE:
		ret = MESSAGE_START_BATCH_RESPONSE
	case kproto.Command_END_BATCH:
		ret = MESSAGE_END_BATCH
	case kproto.Command_END_BATCH_RESPONSE:
		ret = MESSAGE_END_BATCH_RESPONSE
	case kproto.Command_ABORT_BATCH:
		ret = MESSAGE_ABORT_BATCH
	case kproto.Command_ABORT_BATCH_RESPONSE:
		ret = MESSAGE_ABORT_BATCH_RESPONSE
	}
	return ret
}

// Algorithm
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
	str, ok := strAlgorithm[a]
	if ok {
		return str
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

// Synchronization allows the puts and deletes to determine if they are to be
// SYNC_WRITETHROUGH: This request is made persistent before returning. This does not effect any other pending operations.
// SYNC_WRITEBACK: They can be made persistent when the device chooses, or when a subsequent FLUSH is give to the device.
// SYNC_FLUSH: All pending information that has not been written is pushed to the disk and the command that specifies
// FLUSH is written last and then returned. All WRITEBACK writes that have received ending status will be guaranteed
// to be written before the FLUSH operation is returned completed.
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
	str, ok := strSynchronization[sync]
	if ok {
		return str
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
	str, ok := strPriority[p]
	if ok {
		return str
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

type MediaOperation struct {
	StartKey          []byte
	EndKey            []byte
	StartKeyInclusive bool
	EndKeyInclusive   bool
}

type ACLPermission int32

const (
	_                       ACLPermission = iota
	ACL_PERMISSION_READ     ACLPermission = iota // Can read key/values
	ACL_PERMISSION_WRITE    ACLPermission = iota // Can write key/values
	ACL_PERMISSION_DELETE   ACLPermission = iota // Can delete key/values
	ACL_PERMISSION_RANGE    ACLPermission = iota // Can do a range
	ACL_PERMISSION_SETUP    ACLPermission = iota // Can setup a device
	ACL_PERMISSION_P2POP    ACLPermission = iota // Can do a peer to peer operation
	ACL_PERMISSION_GETLOG   ACLPermission = iota // Can get log
	ACL_PERMISSION_SECURITY ACLPermission = iota // Can set up the security of device
)

var strACLPermission = map[ACLPermission]string{
	ACL_PERMISSION_READ:     "ACL_PERMISSION_READ",
	ACL_PERMISSION_WRITE:    "ACL_PERMISSION_WRITE",
	ACL_PERMISSION_DELETE:   "ACL_PERMISSION_DELETE",
	ACL_PERMISSION_RANGE:    "ACL_PERMISSION_RANGE",
	ACL_PERMISSION_SETUP:    "ACL_PERMISSION_SETUP",
	ACL_PERMISSION_P2POP:    "ACL_PERMISSION_P2POP",
	ACL_PERMISSION_GETLOG:   "ACL_PERMISSION_GETLOG",
	ACL_PERMISSION_SECURITY: "ACL_PERMISSION_SECURITY",
}

func (p ACLPermission) String() string {
	str, ok := strACLPermission[p]
	if ok {
		return str
	}
	return "Unknown Permission"
}

func convertACLPermissionToProto(perm ACLPermission) kproto.Command_Security_ACL_Permission {
	ret := kproto.Command_Security_ACL_INVALID_PERMISSION
	switch perm {
	case ACL_PERMISSION_READ:
		ret = kproto.Command_Security_ACL_READ
	case ACL_PERMISSION_WRITE:
		ret = kproto.Command_Security_ACL_WRITE
	case ACL_PERMISSION_DELETE:
		ret = kproto.Command_Security_ACL_DELETE
	case ACL_PERMISSION_RANGE:
		ret = kproto.Command_Security_ACL_RANGE
	case ACL_PERMISSION_SETUP:
		ret = kproto.Command_Security_ACL_SETUP
	case ACL_PERMISSION_P2POP:
		ret = kproto.Command_Security_ACL_P2POP
	case ACL_PERMISSION_GETLOG:
		ret = kproto.Command_Security_ACL_GETLOG
	case ACL_PERMISSION_SECURITY:
		ret = kproto.Command_Security_ACL_SECURITY
	}
	return ret
}

func convertACLPermissionFromProto(perm kproto.Command_Security_ACL_Permission) ACLPermission {
	var ret ACLPermission
	switch perm {
	case kproto.Command_Security_ACL_READ:
		ret = ACL_PERMISSION_READ
	case kproto.Command_Security_ACL_WRITE:
		ret = ACL_PERMISSION_WRITE
	case kproto.Command_Security_ACL_DELETE:
		ret = ACL_PERMISSION_DELETE
	case kproto.Command_Security_ACL_RANGE:
		ret = ACL_PERMISSION_RANGE
	case kproto.Command_Security_ACL_SETUP:
		ret = ACL_PERMISSION_SETUP
	case kproto.Command_Security_ACL_P2POP:
		ret = ACL_PERMISSION_P2POP
	case kproto.Command_Security_ACL_GETLOG:
		ret = ACL_PERMISSION_GETLOG
	case kproto.Command_Security_ACL_SECURITY:
		ret = ACL_PERMISSION_SECURITY
	}
	return ret
}

type ACLAlgorithm int32

const (
	_                      ACLAlgorithm = iota
	ACL_ALGORITHM_HMACSHA1 ACLAlgorithm = iota
)

var strACLAlgorithm = map[ACLAlgorithm]string{
	ACL_ALGORITHM_HMACSHA1: "ACL_ALGORITHM_HMACSHA1",
}

func (p ACLAlgorithm) String() string {
	str, ok := strACLAlgorithm[p]
	if ok {
		return str
	}
	return "Unknown ACL HMAC Algorithm"
}

func convertACLAlgorithmToProto(algo ACLAlgorithm) kproto.Command_Security_ACL_HMACAlgorithm {
	ret := kproto.Command_Security_ACL_INVALID_HMAC_ALGORITHM
	switch algo {
	case ACL_ALGORITHM_HMACSHA1:
		ret = kproto.Command_Security_ACL_HmacSHA1
	}
	return ret
}

type ACLScope struct {
	Offset      int64
	Value       []byte
	Permissions []ACLPermission
	TlsRequired bool
}

type ACL struct {
	Identify    int64
	Key         []byte
	Algo        ACLAlgorithm
	Scopes      []ACLScope
	MaxPriority Priority
}

// P2PPushOperation
type P2PPushOperation struct {
	Key     []byte // Key for the object to push to peer kinetic device
	Version []byte
	NewKey  []byte // NewKey to be used for the object on peer kinetic device, if not specify, will be same as Key
	Force   bool
	Request *P2PPushRequest // Chain P2PPushRequest, which will perform on peer kinetic device
}

// P2PPushRequest
type P2PPushRequest struct {
	HostName   string // Peer kinetic device IP / hostname
	Port       int32  // Peer kinetic drvice port
	Tls        bool
	Operations []P2PPushOperation // List of operations to perform on peer kinetic device
}

// P2PPushStatus holds the status for P2PPushOperations.
// AllOperationsSucceeded indicates whether all operations have Status SUCCESS
// When false, clients should traverse operation status codes to discover error cases.
// When true, no further error checking should be required.
type P2PPushStatus struct {
	AllOperationsSucceeded bool     // Overall status for all child operations
	PushStatus             []Status // individual operation status
}

// BatchStatus indicates status of all operations in a batch commit.
type BatchStatus struct {
	DoneSequence   []int64 // All sequence Ids of those commands (PUT/DELETE) performed successfully in the batch
	FailedSequence int64   // Non 0 value means the first failed operation sequence in the batch, 0 means no failure
}
