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

/*
Package kinetic is golang kinetic client implementation.

For details about kinetic protocol, please refer to https://github.com/Kinetic/kinetic-protocol
*/
package kinetic

import (
	"io"
	"os"

	kproto "github.com/Kinetic/kinetic-go/proto"
	"github.com/Sirupsen/logrus"
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
	// LogLevelPanic level. Panic.
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

// SetLogLevel sets kinetic library log level
func SetLogLevel(l LogLevel) {
	klog.Level = logrus.Level(l)
}

// SetLogOutput sets kinetic library log output
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

// MessageType defines the top level kinetic command message type.
type MessageType int32

// MessageType for each message exchanged between kinetic device and client
const (
	_                            MessageType = iota
	MessageGet                   MessageType = iota
	MessageGetResponse           MessageType = iota
	MessagePut                   MessageType = iota
	MessagePutResponse           MessageType = iota
	MessageDelete                MessageType = iota
	MessageDeleteResponse        MessageType = iota
	MessageGetNext               MessageType = iota
	MessageGetNextResponse       MessageType = iota
	MessageGetPrevious           MessageType = iota
	MessageGetPreviousResponse   MessageType = iota
	MessageGetKeyRange           MessageType = iota
	MessageGetKeyRangeResponse   MessageType = iota
	MessageGetVersion            MessageType = iota
	MessageGetVersionResponse    MessageType = iota
	MessageSetup                 MessageType = iota
	MessageSetupResponse         MessageType = iota
	MessageGetLog                MessageType = iota
	MessageGetLogResponse        MessageType = iota
	MessageSecurity              MessageType = iota
	MessageSecurityResponse      MessageType = iota
	MessagePeer2PeerPush         MessageType = iota
	MessagePeer2PeerPushResponse MessageType = iota
	MessageNoop                  MessageType = iota
	MessageNoopResponse          MessageType = iota
	MessageFlushAllData          MessageType = iota
	MessageFlushAllDataResponse  MessageType = iota
	MessagePinOp                 MessageType = iota
	MessagePinOpResponse         MessageType = iota
	MessageMediaScan             MessageType = iota
	MessageMediaScanResponse     MessageType = iota
	MessageMediaOptimize         MessageType = iota
	MessageMediaOptimizeResponse MessageType = iota
	MessageStartBatch            MessageType = iota
	MessageStartBatchResponse    MessageType = iota
	MessageEndBatch              MessageType = iota
	MessageEndBatchResponse      MessageType = iota
	MessageAbortBatch            MessageType = iota
	MessageAbortBatchResponse    MessageType = iota
	MessageSetPowerLevel         MessageType = iota
	MessageSetPowerLevelResponse MessageType = iota
)

var strMessageType = map[MessageType]string{
	MessageGet:                   "GET",
	MessageGetResponse:           "GET_RESPONSE",
	MessagePut:                   "PUT",
	MessagePutResponse:           "PUT_RESPONSE",
	MessageDelete:                "DELETE",
	MessageDeleteResponse:        "DELETE_RESPONSE",
	MessageGetNext:               "GETNEXT",
	MessageGetNextResponse:       "GETNEXT_RESPONSE",
	MessageGetPrevious:           "GETPREVIOUS",
	MessageGetPreviousResponse:   "GETPREVIOUS_RESPONSE",
	MessageGetKeyRange:           "GETKEYRANGE",
	MessageGetKeyRangeResponse:   "GETKEYRANGE_RESPONSE",
	MessageGetVersion:            "GETVERSION",
	MessageGetVersionResponse:    "GETVERSION_RESPONSE",
	MessageSetup:                 "SETUP",
	MessageSetupResponse:         "SETUP_RESPONSE",
	MessageGetLog:                "GETLOG",
	MessageGetLogResponse:        "GETLOG_RESPONSE",
	MessageSecurity:              "SECURITY",
	MessageSecurityResponse:      "SECURITY_RESPONSE",
	MessagePeer2PeerPush:         "PEER2PEERPUSH",
	MessagePeer2PeerPushResponse: "PEER2PEERPUSH_RESPONSE",
	MessageNoop:                  "NOOP",
	MessageNoopResponse:          "NOOP_RESPONSE",
	MessageFlushAllData:          "FLUSHALLDATA",
	MessageFlushAllDataResponse:  "FLUSHALLDATA_RESPONSE",
	MessagePinOp:                 "PINOP",
	MessagePinOpResponse:         "PINOP_RESPONSE",
	MessageMediaScan:             "MEDIASCAN",
	MessageMediaScanResponse:     "MEDIASCAN_RESPONSE",
	MessageMediaOptimize:         "MEDIAOPTIMIZE",
	MessageMediaOptimizeResponse: "MEDIAOPTIMIZE_RESPONSE",
	MessageStartBatch:            "START_BATCH",
	MessageStartBatchResponse:    "START_BATCH_RESPONSE",
	MessageEndBatch:              "END_BATCH",
	MessageEndBatchResponse:      "END_BATCH_RESPONSE",
	MessageAbortBatch:            "ABORT_BATCH",
	MessageAbortBatchResponse:    "ABORT_BATCH_RESPONSE",
	MessageSetPowerLevel:         "SET_POWER_LEVEL",
	MessageSetPowerLevelResponse: "SET_POWER_LEVEL_RESPONSE",
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
	case MessageGet:
		ret = kproto.Command_GET
	case MessageGetResponse:
		ret = kproto.Command_GET_RESPONSE
	case MessagePut:
		ret = kproto.Command_PUT
	case MessagePutResponse:
		ret = kproto.Command_PUT_RESPONSE
	case MessageDelete:
		ret = kproto.Command_DELETE
	case MessageDeleteResponse:
		ret = kproto.Command_DELETE_RESPONSE
	case MessageGetNext:
		ret = kproto.Command_GETNEXT
	case MessageGetNextResponse:
		ret = kproto.Command_GETNEXT_RESPONSE
	case MessageGetPrevious:
		ret = kproto.Command_GETPREVIOUS
	case MessageGetPreviousResponse:
		ret = kproto.Command_GETPREVIOUS_RESPONSE
	case MessageGetKeyRange:
		ret = kproto.Command_GETKEYRANGE
	case MessageGetKeyRangeResponse:
		ret = kproto.Command_GETKEYRANGE_RESPONSE
	case MessageGetVersion:
		ret = kproto.Command_GETVERSION
	case MessageGetVersionResponse:
		ret = kproto.Command_GETVERSION_RESPONSE
	case MessageSetup:
		ret = kproto.Command_SETUP
	case MessageSetupResponse:
		ret = kproto.Command_SETUP_RESPONSE
	case MessageGetLog:
		ret = kproto.Command_GETLOG
	case MessageGetLogResponse:
		ret = kproto.Command_GETLOG_RESPONSE
	case MessageSecurity:
		ret = kproto.Command_SECURITY
	case MessageSecurityResponse:
		ret = kproto.Command_SECURITY_RESPONSE
	case MessagePeer2PeerPush:
		ret = kproto.Command_PEER2PEERPUSH
	case MessagePeer2PeerPushResponse:
		ret = kproto.Command_PEER2PEERPUSH_RESPONSE
	case MessageNoop:
		ret = kproto.Command_NOOP
	case MessageNoopResponse:
		ret = kproto.Command_NOOP_RESPONSE
	case MessageFlushAllData:
		ret = kproto.Command_FLUSHALLDATA
	case MessageFlushAllDataResponse:
		ret = kproto.Command_FLUSHALLDATA_RESPONSE
	case MessagePinOp:
		ret = kproto.Command_PINOP
	case MessagePinOpResponse:
		ret = kproto.Command_PINOP_RESPONSE
	case MessageMediaScan:
		ret = kproto.Command_MEDIASCAN
	case MessageMediaScanResponse:
		ret = kproto.Command_MEDIASCAN_RESPONSE
	case MessageMediaOptimize:
		ret = kproto.Command_MEDIAOPTIMIZE
	case MessageMediaOptimizeResponse:
		ret = kproto.Command_MEDIAOPTIMIZE_RESPONSE
	case MessageStartBatch:
		ret = kproto.Command_START_BATCH
	case MessageStartBatchResponse:
		ret = kproto.Command_START_BATCH_RESPONSE
	case MessageEndBatch:
		ret = kproto.Command_END_BATCH
	case MessageEndBatchResponse:
		ret = kproto.Command_END_BATCH_RESPONSE
	case MessageAbortBatch:
		ret = kproto.Command_ABORT_BATCH
	case MessageAbortBatchResponse:
		ret = kproto.Command_ABORT_BATCH_RESPONSE
	case MessageSetPowerLevel:
		ret = kproto.Command_SET_POWER_LEVEL
	case MessageSetPowerLevelResponse:
		ret = kproto.Command_SET_POWER_LEVEL_RESPONSE
	}
	return ret
}

func convertMessageTypeFromProto(m kproto.Command_MessageType) MessageType {
	var ret MessageType
	switch m {
	case kproto.Command_GET:
		ret = MessageGet
	case kproto.Command_GET_RESPONSE:
		ret = MessageGetResponse
	case kproto.Command_PUT:
		ret = MessagePut
	case kproto.Command_PUT_RESPONSE:
		ret = MessagePutResponse
	case kproto.Command_DELETE:
		ret = MessageDelete
	case kproto.Command_DELETE_RESPONSE:
		ret = MessageDeleteResponse
	case kproto.Command_GETNEXT:
		ret = MessageGetNext
	case kproto.Command_GETNEXT_RESPONSE:
		ret = MessageGetNextResponse
	case kproto.Command_GETPREVIOUS:
		ret = MessageGetPrevious
	case kproto.Command_GETPREVIOUS_RESPONSE:
		ret = MessageGetPreviousResponse
	case kproto.Command_GETKEYRANGE:
		ret = MessageGetKeyRange
	case kproto.Command_GETKEYRANGE_RESPONSE:
		ret = MessageGetKeyRangeResponse
	case kproto.Command_GETVERSION:
		ret = MessageGetVersion
	case kproto.Command_GETVERSION_RESPONSE:
		ret = MessageGetVersionResponse
	case kproto.Command_SETUP:
		ret = MessageSetup
	case kproto.Command_SETUP_RESPONSE:
		ret = MessageSetupResponse
	case kproto.Command_GETLOG:
		ret = MessageGetLog
	case kproto.Command_GETLOG_RESPONSE:
		ret = MessageGetLogResponse
	case kproto.Command_SECURITY:
		ret = MessageSecurity
	case kproto.Command_SECURITY_RESPONSE:
		ret = MessageSecurityResponse
	case kproto.Command_PEER2PEERPUSH:
		ret = MessagePeer2PeerPush
	case kproto.Command_PEER2PEERPUSH_RESPONSE:
		ret = MessagePeer2PeerPushResponse
	case kproto.Command_NOOP:
		ret = MessageNoop
	case kproto.Command_NOOP_RESPONSE:
		ret = MessageNoopResponse
	case kproto.Command_FLUSHALLDATA:
		ret = MessageFlushAllData
	case kproto.Command_FLUSHALLDATA_RESPONSE:
		ret = MessageFlushAllDataResponse
	case kproto.Command_PINOP:
		ret = MessagePinOp
	case kproto.Command_PINOP_RESPONSE:
		ret = MessagePinOpResponse
	case kproto.Command_MEDIASCAN:
		ret = MessageMediaScan
	case kproto.Command_MEDIASCAN_RESPONSE:
		ret = MessageMediaScanResponse
	case kproto.Command_MEDIAOPTIMIZE:
		ret = MessageMediaOptimize
	case kproto.Command_MEDIAOPTIMIZE_RESPONSE:
		ret = MessageMediaOptimizeResponse
	case kproto.Command_START_BATCH:
		ret = MessageStartBatch
	case kproto.Command_START_BATCH_RESPONSE:
		ret = MessageStartBatchResponse
	case kproto.Command_END_BATCH:
		ret = MessageEndBatch
	case kproto.Command_END_BATCH_RESPONSE:
		ret = MessageEndBatchResponse
	case kproto.Command_ABORT_BATCH:
		ret = MessageAbortBatch
	case kproto.Command_ABORT_BATCH_RESPONSE:
		ret = MessageAbortBatchResponse
	case kproto.Command_SET_POWER_LEVEL:
		ret = MessageSetPowerLevel
	case kproto.Command_SET_POWER_LEVEL_RESPONSE:
		ret = MessageSetPowerLevelResponse
	}
	return ret
}

// Algorithm defines the which algorithm used to protect data.
type Algorithm int32

// Algorithm to protect data
const (
	_               Algorithm = iota
	AlgorithmSHA1   Algorithm = iota
	AlgorithmSHA2   Algorithm = iota
	AlgorithmSHA3   Algorithm = iota
	AlgorithmCRC32C Algorithm = iota
	AlgorithmCRC64  Algorithm = iota
	AlgorithmCRC32  Algorithm = iota
)

var strAlgorithm = map[Algorithm]string{
	AlgorithmSHA1:   "Algorithm SHA1",
	AlgorithmSHA2:   "Algorithm SHA2",
	AlgorithmSHA3:   "Algorithm SHA3",
	AlgorithmCRC32C: "Algorithm CRC32C",
	AlgorithmCRC64:  "Algorithm CRC64",
	AlgorithmCRC32:  "Algorithm CRC32",
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
	case AlgorithmSHA1:
		ret = kproto.Command_SHA1
	case AlgorithmSHA2:
		ret = kproto.Command_SHA2
	case AlgorithmSHA3:
		ret = kproto.Command_SHA3
	case AlgorithmCRC32C:
		ret = kproto.Command_CRC32C
	case AlgorithmCRC64:
		ret = kproto.Command_CRC64
	case AlgorithmCRC32:
		ret = kproto.Command_CRC32
	}
	return ret
}

func convertAlgoFromProto(a kproto.Command_Algorithm) Algorithm {
	var ret Algorithm
	switch a {
	case kproto.Command_SHA1:
		ret = AlgorithmSHA1
	case kproto.Command_SHA2:
		ret = AlgorithmSHA2
	case kproto.Command_SHA3:
		ret = AlgorithmSHA3
	case kproto.Command_CRC32C:
		ret = AlgorithmCRC32C
	case kproto.Command_CRC64:
		ret = AlgorithmCRC64
	case kproto.Command_CRC32:
		ret = AlgorithmCRC32
	}
	return ret
}

// Synchronization allows the puts and deletes to determine how to make data persistent.
type Synchronization int32

// Syncchronization types
// SyncWriteThrough: This request is made persistent before returning. This does not effect any other pending operations.
// SyncWriteBack: They can be made persistent when the device chooses, or when a subsequent FLUSH is give to the device.
// SyncFlush: All pending information that has not been written is pushed to the disk and the command that specifies
// FLUSH is written last and then returned. All WRITEBACK writes that have received ending status will be guaranteed
// to be written before the FLUSH operation is returned completed.
const (
	_                Synchronization = iota
	SyncWriteThrough Synchronization = iota
	SyncWriteBack    Synchronization = iota
	SyncFlush        Synchronization = iota
)

var strSynchronization = map[Synchronization]string{
	SyncWriteThrough: "SYNC_WRITETHROUGH",
	SyncWriteBack:    "SYNC_WRITEBACK",
	SyncFlush:        "SYNC_FLUSH",
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
	case SyncWriteThrough:
		ret = kproto.Command_WRITETHROUGH
	case SyncWriteBack:
		ret = kproto.Command_WRITEBACK
	case SyncFlush:
		ret = kproto.Command_FLUSH
	}
	return ret
}

func convertSyncFromProto(sync kproto.Command_Synchronization) Synchronization {
	var ret Synchronization
	switch sync {
	case kproto.Command_WRITETHROUGH:
		ret = SyncWriteThrough
	case kproto.Command_WRITEBACK:
		ret = SyncWriteBack
	case kproto.Command_FLUSH:
		ret = SyncFlush
	}
	return ret
}

// Priority is a simple integer that determines the priority of this
// request. All activity at a higher priority will execute before that
// of lower priority traffic. A higher number is higher priority.
type Priority int32

// Priority level from lowest to highest.
const (
	_               Priority = iota
	PriorityLowest  Priority = iota
	PriorityLower   Priority = iota
	PriorityNormal  Priority = iota
	PriorityHigher  Priority = iota
	PriorityHighest Priority = iota
)

var strPriority = map[Priority]string{
	PriorityLowest:  "PRIORITY_LOWEST",
	PriorityLower:   "PRIORITY_LOWER",
	PriorityNormal:  "PRIORITY_NORMAL",
	PriorityHigher:  "PRIORITY_HIGHER",
	PriorityHighest: "PRIORITY_HIGHEST",
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
	case PriorityLowest:
		ret = kproto.Command_LOWEST
	case PriorityLower:
		ret = kproto.Command_LOWER
	case PriorityNormal:
		ret = kproto.Command_NORMAL
	case PriorityHigher:
		ret = kproto.Command_HIGHER
	case PriorityHighest:
		ret = kproto.Command_HIGHEST
	}
	return ret
}

func convertPriorityFromProto(p kproto.Command_Priority) Priority {
	ret := PriorityNormal
	switch p {
	case kproto.Command_LOWEST:
		ret = PriorityLowest
	case kproto.Command_LOWER:
		ret = PriorityLower
	case kproto.Command_NORMAL:
		ret = PriorityNormal
	case kproto.Command_HIGHER:
		ret = PriorityHigher
	case kproto.Command_HIGHEST:
		ret = PriorityHighest
	}
	return ret
}

// Record structure defines information for an object stored on kinetic device.
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

// KeyRange structure defines the range for GetRange operation.
type KeyRange struct {
	StartKey          []byte
	EndKey            []byte
	StartKeyInclusive bool
	EndKeyInclusive   bool
	Reverse           bool
	Max               int32
}

// MediaOperation structure defines media operation information for MediaScan and MediaOptimize.
type MediaOperation struct {
	StartKey          []byte
	EndKey            []byte
	StartKeyInclusive bool
	EndKeyInclusive   bool
}

// ACLPermission defines what operations a user identity can perform.
type ACLPermission int32

// ACLPermission for various type of operation.
const (
	_                            ACLPermission = iota
	ACLPermissionRead            ACLPermission = iota // Can read key/values
	ACLPermissionWrite           ACLPermission = iota // Can write key/values
	ACLPermissionDelete          ACLPermission = iota // Can delete key/values
	ACLPermissionRange           ACLPermission = iota // Can do a range
	ACLPermissionSetup           ACLPermission = iota // Can setup a device
	ACLPermissionP2POP           ACLPermission = iota // Can do a peer to peer operation
	ACLPermissionGetLog          ACLPermission = iota // Can get log
	ACLPermissionSecurity        ACLPermission = iota // Can set up the security of device
	ACLPermissionPowerManagement ACLPermission = iota // Can set power level
)

var strACLPermission = map[ACLPermission]string{
	ACLPermissionRead:            "ACL_PERMISSION_READ",
	ACLPermissionWrite:           "ACL_PERMISSION_WRITE",
	ACLPermissionDelete:          "ACL_PERMISSION_DELETE",
	ACLPermissionRange:           "ACL_PERMISSION_RANGE",
	ACLPermissionSetup:           "ACL_PERMISSION_SETUP",
	ACLPermissionP2POP:           "ACL_PERMISSION_P2POP",
	ACLPermissionGetLog:          "ACL_PERMISSION_GETLOG",
	ACLPermissionSecurity:        "ACL_PERMISSION_SECURITY",
	ACLPermissionPowerManagement: "ACL_PERMISSION_POWER_MANAGEMENT",
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
	case ACLPermissionRead:
		ret = kproto.Command_Security_ACL_READ
	case ACLPermissionWrite:
		ret = kproto.Command_Security_ACL_WRITE
	case ACLPermissionDelete:
		ret = kproto.Command_Security_ACL_DELETE
	case ACLPermissionRange:
		ret = kproto.Command_Security_ACL_RANGE
	case ACLPermissionSetup:
		ret = kproto.Command_Security_ACL_SETUP
	case ACLPermissionP2POP:
		ret = kproto.Command_Security_ACL_P2POP
	case ACLPermissionGetLog:
		ret = kproto.Command_Security_ACL_GETLOG
	case ACLPermissionSecurity:
		ret = kproto.Command_Security_ACL_SECURITY
	case ACLPermissionPowerManagement:
		ret = kproto.Command_Security_ACL_POWER_MANAGEMENT
	}
	return ret
}

func convertACLPermissionFromProto(perm kproto.Command_Security_ACL_Permission) ACLPermission {
	var ret ACLPermission
	switch perm {
	case kproto.Command_Security_ACL_READ:
		ret = ACLPermissionRead
	case kproto.Command_Security_ACL_WRITE:
		ret = ACLPermissionWrite
	case kproto.Command_Security_ACL_DELETE:
		ret = ACLPermissionDelete
	case kproto.Command_Security_ACL_RANGE:
		ret = ACLPermissionRange
	case kproto.Command_Security_ACL_SETUP:
		ret = ACLPermissionSetup
	case kproto.Command_Security_ACL_P2POP:
		ret = ACLPermissionP2POP
	case kproto.Command_Security_ACL_GETLOG:
		ret = ACLPermissionGetLog
	case kproto.Command_Security_ACL_SECURITY:
		ret = ACLPermissionSecurity
	case kproto.Command_Security_ACL_POWER_MANAGEMENT:
		ret = ACLPermissionPowerManagement
	}
	return ret
}

// ACLAlgorithm defines the HMAC algorithm.
type ACLAlgorithm int32

// ACLAlgorithm values.
const (
	_                    ACLAlgorithm = iota
	ACLAlgorithmHMACSHA1 ACLAlgorithm = iota
)

var strACLAlgorithm = map[ACLAlgorithm]string{
	ACLAlgorithmHMACSHA1: "ACL_ALGORITHM_HMACSHA1",
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
	case ACLAlgorithmHMACSHA1:
		ret = kproto.Command_Security_ACL_HmacSHA1
	}
	return ret
}

// ACLScope defines scope of ACL.
type ACLScope struct {
	Offset      int64
	Value       []byte
	Permissions []ACLPermission
	TLSRequired bool
}

// ACL structure for SetACL call. Defines permission for identity.
type ACL struct {
	Identity    int64
	Key         []byte
	Algo        ACLAlgorithm
	Scopes      []ACLScope
	MaxPriority Priority
}

// P2PPushOperation structure for P2PPush operation.
type P2PPushOperation struct {
	Key     []byte // Key for the object to push to peer kinetic device
	Version []byte
	NewKey  []byte // NewKey to be used for the object on peer kinetic device, if not specify, will be same as Key
	Force   bool
	Request *P2PPushRequest // Chain P2PPushRequest, which will perform on peer kinetic device
}

// P2PPushRequest structure for P2PPush operation
type P2PPushRequest struct {
	HostName   string // Peer kinetic device IP / hostname
	Port       int32  // Peer kinetic drvice port
	TLS        bool
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

// PowerLevel defines the power level of kinetic device.
type PowerLevel int32

// PowerLevel values.
const (
	_                     PowerLevel = iota
	PowerLevelOperational PowerLevel = iota
	PowerLevelHibernate   PowerLevel = iota
	PowerLevelShutdown    PowerLevel = iota
	PowerLevelFail        PowerLevel = iota
)

var strPowerLevel = map[PowerLevel]string{
	PowerLevelOperational: "OPERATIONAL",
	PowerLevelHibernate:   "HIBERNATE",
	PowerLevelShutdown:    "SHUTDOWN",
	PowerLevelFail:        "FAIL",
}

func (p PowerLevel) String() string {
	str, ok := strPowerLevel[p]
	if ok {
		return str
	}
	return "Unknown Power Level"
}

func convertPowerLevelToProto(p PowerLevel) kproto.Command_PowerLevel {
	var ret kproto.Command_PowerLevel
	switch p {
	case PowerLevelOperational:
		ret = kproto.Command_OPERATIONAL
	case PowerLevelHibernate:
		ret = kproto.Command_HIBERNATE
	case PowerLevelShutdown:
		ret = kproto.Command_SHUTDOWN
	case PowerLevelFail:
		ret = kproto.Command_FAIL
	}
	return ret
}

func convertPowerLevelFromProto(p kproto.Command_PowerLevel) PowerLevel {
	var ret PowerLevel
	switch p {
	case kproto.Command_OPERATIONAL:
		ret = PowerLevelOperational
	case kproto.Command_HIBERNATE:
		ret = PowerLevelHibernate
	case kproto.Command_SHUTDOWN:
		ret = PowerLevelShutdown
	case kproto.Command_FAIL:
		ret = PowerLevelFail
	}
	return ret
}
