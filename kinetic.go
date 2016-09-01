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

// algorithm
type Algorithm int32

const (
	_          Algorithm = -1
	ALGO_SHA1  Algorithm = 1
	ALGO_SHA2  Algorithm = 2
	ALGO_SHA3  Algorithm = 3
	ALGO_CRC32 Algorithm = 4
	ALGO_CRC64 Algorithm = 5
)

type Synchronization int32

const (
	_                 Synchronization = -1
	SYNC_WRITETHROUGH Synchronization = 1
	SYNC_WRITEBACK    Synchronization = 2
	SYNC_FLUSH        Synchronization = 3
)

type Priority int32

const (
	_                Priority = iota
	PRIORITY_LOWEST  Priority = iota
	PRIORITY_LOWER   Priority = iota
	PRIORITY_NORMAL  Priority = iota
	PRIORITY_HIGHER  Priority = iota
	PRIORITY_HIGHEST Priority = iota
)

type LogType int32

const (
	_                 LogType = iota
	LOG_UTILIZATIONS  LogType = iota
	LOG_TEMPERATURES  LogType = iota
	LOG_CAPACITIES    LogType = iota
	LOG_CONFIGURATION LogType = iota
	LOG_STATISTICS    LogType = iota
	LOG_MESSAGES      LogType = iota
	LOG_LIMITS        LogType = iota
	LOG_DEVICE        LogType = iota
)

type ACL int32

const (
	_            ACL = iota
	ACL_READ     ACL = iota
	ACL_WRITE    ACL = iota
	ACL_DELETE   ACL = iota
	ACL_RANGE    ACL = iota
	ACL_SETUP    ACL = iota
	ACL_P2POP    ACL = iota
	ACL_GETLOG   ACL = iota
	ACL_SECURITY ACL = iota
)

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

func convertLogTypeToProto(l LogType) kproto.Command_GetLog_Type {
	ret := kproto.Command_GetLog_INVALID_TYPE
	switch l {
	case LOG_UTILIZATIONS:
		ret = kproto.Command_GetLog_UTILIZATIONS
	case LOG_TEMPERATURES:
		ret = kproto.Command_GetLog_TEMPERATURES
	case LOG_CAPACITIES:
		ret = kproto.Command_GetLog_CAPACITIES
	case LOG_CONFIGURATION:
		ret = kproto.Command_GetLog_CONFIGURATION
	case LOG_STATISTICS:
		ret = kproto.Command_GetLog_STATISTICS
	case LOG_MESSAGES:
		ret = kproto.Command_GetLog_MESSAGES
	case LOG_LIMITS:
		ret = kproto.Command_GetLog_LIMITS
	case LOG_DEVICE:
		ret = kproto.Command_GetLog_DEVICE
	}
	return ret
}

func convertLogTypeFromProto(l kproto.Command_GetLog_Type) LogType {
	var ret LogType
	switch l {
	case kproto.Command_GetLog_UTILIZATIONS:
		ret = LOG_UTILIZATIONS
	case kproto.Command_GetLog_TEMPERATURES:
		ret = LOG_TEMPERATURES
	case kproto.Command_GetLog_CAPACITIES:
		ret = LOG_CAPACITIES
	case kproto.Command_GetLog_CONFIGURATION:
		ret = LOG_CONFIGURATION
	case kproto.Command_GetLog_STATISTICS:
		ret = LOG_STATISTICS
	case kproto.Command_GetLog_MESSAGES:
		ret = LOG_MESSAGES
	case kproto.Command_GetLog_LIMITS:
		ret = LOG_LIMITS
	case kproto.Command_GetLog_DEVICE:
		ret = LOG_DEVICE
	}
	return ret
}

func convertStatusCodeToProto(s StatusCode) kproto.Command_Status_StatusCode {
	ret := kproto.Command_Status_INVALID_STATUS_CODE
	switch s {
	case REMOTE_NOT_ATTEMPTED:
		ret = kproto.Command_Status_NOT_ATTEMPTED
	case OK:
		ret = kproto.Command_Status_SUCCESS
	case REMOTE_HMAC_ERROR:
		ret = kproto.Command_Status_HMAC_FAILURE
	case REMOTE_NOT_AUTHORIZED:
		ret = kproto.Command_Status_NOT_AUTHORIZED
	case REMOTE_CLUSTER_VERSION_MISMATCH:
		ret = kproto.Command_Status_VERSION_FAILURE
	case REMOTE_INTERNAL_ERROR:
		ret = kproto.Command_Status_INTERNAL_ERROR
	case REMOTE_HEADER_REQUIRED:
		ret = kproto.Command_Status_HEADER_REQUIRED
	case REMOTE_NOT_FOUND:
		ret = kproto.Command_Status_NOT_FOUND
	case REMOTE_VERSION_MISMATCH:
		ret = kproto.Command_Status_VERSION_MISMATCH
	case REMOTE_SERVICE_BUSY:
		ret = kproto.Command_Status_SERVICE_BUSY
	case REMOTE_EXPIRED:
		ret = kproto.Command_Status_EXPIRED
	case REMOTE_DATA_ERROR:
		ret = kproto.Command_Status_DATA_ERROR
	case REMOTE_PERM_DATA_ERROR:
		ret = kproto.Command_Status_PERM_DATA_ERROR
	case REMOTE_CONNECTION_ERROR:
		ret = kproto.Command_Status_REMOTE_CONNECTION_ERROR
	case REMOTE_NO_SPACE:
		ret = kproto.Command_Status_NO_SPACE
	case REMOTE_NO_SUCH_HMAC_ALGORITHM:
		ret = kproto.Command_Status_NO_SUCH_HMAC_ALGORITHM
	case REMOTE_INVALID_REQUEST:
		ret = kproto.Command_Status_INVALID_REQUEST
	case REMOTE_NESTED_OPERATION_ERRORS:
		ret = kproto.Command_Status_NESTED_OPERATION_ERRORS
	case REMOTE_DEVICE_LOCKED:
		ret = kproto.Command_Status_DEVICE_LOCKED
	case REMOTE_DEVICE_ALREADY_UNLOCKED:
		ret = kproto.Command_Status_DEVICE_ALREADY_UNLOCKED
	case REMOTE_CONNECTION_TERMINATED:
		ret = kproto.Command_Status_CONNECTION_TERMINATED
	case REMOTE_INVALID_BATCH:
		ret = kproto.Command_Status_INVALID_BATCH
	}
	return ret
}

func convertStatusCodeFromProto(s kproto.Command_Status_StatusCode) StatusCode {
	ret := REMOTE_OTHER_ERROR
	switch s {
	case kproto.Command_Status_NOT_ATTEMPTED:
		ret = REMOTE_NOT_ATTEMPTED
	case kproto.Command_Status_SUCCESS:
		ret = OK
	case kproto.Command_Status_HMAC_FAILURE:
		ret = REMOTE_HMAC_ERROR
	case kproto.Command_Status_NOT_AUTHORIZED:
		ret = REMOTE_NOT_AUTHORIZED
	case kproto.Command_Status_VERSION_FAILURE:
		ret = REMOTE_CLUSTER_VERSION_MISMATCH
	case kproto.Command_Status_INTERNAL_ERROR:
		ret = REMOTE_INTERNAL_ERROR
	case kproto.Command_Status_HEADER_REQUIRED:
		ret = REMOTE_HEADER_REQUIRED
	case kproto.Command_Status_NOT_FOUND:
		ret = REMOTE_NOT_FOUND
	case kproto.Command_Status_VERSION_MISMATCH:
		ret = REMOTE_VERSION_MISMATCH
	case kproto.Command_Status_SERVICE_BUSY:
		ret = REMOTE_SERVICE_BUSY
	case kproto.Command_Status_EXPIRED:
		ret = REMOTE_EXPIRED
	case kproto.Command_Status_DATA_ERROR:
		ret = REMOTE_DATA_ERROR
	case kproto.Command_Status_PERM_DATA_ERROR:
		ret = REMOTE_PERM_DATA_ERROR
	case kproto.Command_Status_REMOTE_CONNECTION_ERROR:
		ret = REMOTE_CONNECTION_ERROR
	case kproto.Command_Status_NO_SPACE:
		ret = REMOTE_NO_SPACE
	case kproto.Command_Status_NO_SUCH_HMAC_ALGORITHM:
		ret = REMOTE_NO_SUCH_HMAC_ALGORITHM
	case kproto.Command_Status_INVALID_REQUEST:
		ret = REMOTE_INVALID_REQUEST
	case kproto.Command_Status_NESTED_OPERATION_ERRORS:
		ret = REMOTE_NESTED_OPERATION_ERRORS
	case kproto.Command_Status_DEVICE_LOCKED:
		ret = REMOTE_DEVICE_LOCKED
	case kproto.Command_Status_DEVICE_ALREADY_UNLOCKED:
		ret = REMOTE_DEVICE_ALREADY_UNLOCKED
	case kproto.Command_Status_CONNECTION_TERMINATED:
		ret = REMOTE_CONNECTION_TERMINATED
	case kproto.Command_Status_INVALID_BATCH:
		ret = REMOTE_INVALID_BATCH
	}
	return ret
}

func getStatusFromProto(cmd *kproto.Command) Status {
	code := convertStatusCodeFromProto(cmd.GetStatus().GetCode())
	msg := cmd.GetStatus().GetStatusMessage()

	return Status{code, msg}

	/*
		switch code {
		case CLIENT_IO_ERROR:
			return Status{code, "IO error"}
		case CLIENT_SHUTDOWN:
			return Status{code, "Client shutdown"}
		case PROTOCOL_ERROR_RESPONSE_NO_ACKSEQUENCE:
			return Status{code, "Response did not contain ack sequence"}
		case CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR:
			return Status{code, "Response HMAC verification failed"}
		case REMOTE_HMAC_ERROR:
			return Status{code, "Remote HMAC verification failed"}
		case REMOTE_NOT_AUTHORIZED:
			return Status{code, "Not authorized"}
		case REMOTE_CLUSTER_VERSION_MISMATCH:
			expected_cluster_version := cmd.GetHeader().GetClusterVersion()
			return Status{code, "Cluster version mismatch " + string(expected_cluster_version)}
		case REMOTE_INTERNAL_ERROR:
			return Status{code, "Remote internal error"}
		case REMOTE_HEADER_REQUIRED:
			return Status{code, "Request requires a header to be set"}
		case REMOTE_NOT_FOUND:
			return Status{code, "Key not found"}
		case REMOTE_VERSION_MISMATCH:
			return Status{code, "Version mismatch"}
		case REMOTE_SERVICE_BUSY:
			return Status{code, "Remote service is busy"}
		case REMOTE_EXPIRED:
			return Status{code, "Remote timeout"}
		case REMOTE_DATA_ERROR:
			return Status{code, "Remote transient data error"}
		case REMOTE_PERM_DATA_ERROR:
			return Status{code, "Remote permanent data error"}
		case REMOTE_CONNECTION_ERROR:
			return Status{code, "Remote connection to peer failed"}
		case REMOTE_NO_SPACE:
			return Status{code, "No space left"}
		case REMOTE_NO_SUCH_HMAC_ALGORITHM:
			return Status{code, "Unknown HMAC algorithm"}
		case REMOTE_NESTED_OPERATION_ERRORS:
			return Status{code, "Operation completed but has nested errors"}
		case REMOTE_DEVICE_LOCKED:
			return Status{code, "Remote device is locked"}
		case REMOTE_DEVICE_ALREADY_UNLOCKED:
			return Status{code, "Remote device is already unlocked"}
		case REMOTE_CONNECTION_TERMINATED:
			return Status{code, "Remote connection is terminated"}
		case REMOTE_INVALID_BATCH:
			return Status{code, "Invalid batch"}
		case REMOTE_INVALID_EXECUTE:
			return Status{code, "Invalid execute of applet"}
		case REMOTE_EXECUTE_COMPLETE:
			return Status{code, "Applet execute complete"}
		default:
			return Status{code, "Internal Error"}
		}
	*/
}
