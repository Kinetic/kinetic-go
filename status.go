package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

// Status code for kinetic message.
// Including status code get from device, or client internal error code.
type StatusCode int32

const (
	REMOTE_NOT_ATTEMPTED                    StatusCode = iota
	OK                                      StatusCode = iota
	CLIENT_IO_ERROR                         StatusCode = iota
	CLIENT_SHUTDOWN                         StatusCode = iota
	CLIENT_INTERNAL_ERROR                   StatusCode = iota
	CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR StatusCode = iota
	REMOTE_HMAC_ERROR                       StatusCode = iota
	REMOTE_NOT_AUTHORIZED                   StatusCode = iota
	REMOTE_CLUSTER_VERSION_MISMATCH         StatusCode = iota
	REMOTE_INVALID_REQUEST                  StatusCode = iota
	REMOTE_INTERNAL_ERROR                   StatusCode = iota
	REMOTE_HEADER_REQUIRED                  StatusCode = iota
	REMOTE_NOT_FOUND                        StatusCode = iota
	REMOTE_VERSION_MISMATCH                 StatusCode = iota
	REMOTE_SERVICE_BUSY                     StatusCode = iota
	REMOTE_EXPIRED                          StatusCode = iota
	REMOTE_DATA_ERROR                       StatusCode = iota
	REMOTE_PERM_DATA_ERROR                  StatusCode = iota
	REMOTE_CONNECTION_ERROR                 StatusCode = iota
	REMOTE_NO_SPACE                         StatusCode = iota
	REMOTE_NO_SUCH_HMAC_ALGORITHM           StatusCode = iota
	REMOTE_OTHER_ERROR                      StatusCode = iota
	PROTOCOL_ERROR_RESPONSE_NO_ACKSEQUENCE  StatusCode = iota
	REMOTE_NESTED_OPERATION_ERRORS          StatusCode = iota
	REMOTE_DEVICE_LOCKED                    StatusCode = iota
	REMOTE_DEVICE_ALREADY_UNLOCKED          StatusCode = iota
	REMOTE_CONNECTION_TERMINATED            StatusCode = iota
	REMOTE_INVALID_BATCH                    StatusCode = iota
	REMOTE_INVALID_EXECUTE                  StatusCode = iota
	REMOTE_EXECUTE_COMPLETE                 StatusCode = iota
)

var statusName = map[StatusCode]string{
	REMOTE_NOT_ATTEMPTED:                    "REMOTE_NOT_ATTEMPTED",
	OK:                                      "OK",
	CLIENT_IO_ERROR:                         "CLIENT_IO_ERROR",
	CLIENT_SHUTDOWN:                         "CLIENT_SHUTDOWN",
	CLIENT_INTERNAL_ERROR:                   "CLIENT_INTERNAL_ERROR",
	CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR: "CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR",
	REMOTE_HMAC_ERROR:                       "REMOTE_HMAC_ERROR",
	REMOTE_NOT_AUTHORIZED:                   "REMOTE_NOT_AUTHORIZED",
	REMOTE_CLUSTER_VERSION_MISMATCH:         "REMOTE_CLUSTER_VERSION_MISMATCH",
	REMOTE_INVALID_REQUEST:                  "REMOTE_INVALID_REQUEST",
	REMOTE_INTERNAL_ERROR:                   "REMOTE_INTERNAL_ERROR",
	REMOTE_HEADER_REQUIRED:                  "REMOTE_HEADER_REQUIRED",
	REMOTE_NOT_FOUND:                        "REMOTE_NOT_FOUND",
	REMOTE_VERSION_MISMATCH:                 "REMOTE_VERSION_MISMATCH",
	REMOTE_SERVICE_BUSY:                     "REMOTE_SERVICE_BUSY",
	REMOTE_EXPIRED:                          "REMOTE_EXPIRED",
	REMOTE_DATA_ERROR:                       "REMOTE_DATA_ERROR",
	REMOTE_PERM_DATA_ERROR:                  "REMOTE_PERM_DATA_ERROR",
	REMOTE_CONNECTION_ERROR:                 "REMOTE_CONNECTION_ERROR",
	REMOTE_NO_SPACE:                         "REMOTE_NO_SPACE",
	REMOTE_NO_SUCH_HMAC_ALGORITHM:           "REMOTE_NO_SUCH_HMAC_ALGORITHM",
	REMOTE_OTHER_ERROR:                      "REMOTE_OTHER_ERROR",
	PROTOCOL_ERROR_RESPONSE_NO_ACKSEQUENCE:  "PROTOCOL_ERROR_RESPONSE_NO_ACKSEQUENCE ",
	REMOTE_NESTED_OPERATION_ERRORS:          "REMOTE_NESTED_OPERATION_ERRORS",
	REMOTE_DEVICE_LOCKED:                    "REMOTE_DEVICE_LOCKED",
	REMOTE_DEVICE_ALREADY_UNLOCKED:          "REMOTE_DEVICE_ALREADY_UNLOCKED",
	REMOTE_CONNECTION_TERMINATED:            "REMOTE_CONNECTION_TERMINATED",
	REMOTE_INVALID_BATCH:                    "REMOTE_INVALID_BATCH",
	REMOTE_INVALID_EXECUTE:                  "REMOTE_INVALID_EXECUTE",
	REMOTE_EXECUTE_COMPLETE:                 "REMOTE_EXECUTE_COMPLETE",
}

// Status for each kinetic message.
// Code is the status code and ErrorMsg is the detail message
type Status struct {
	Code     StatusCode
	ErrorMsg string
}

func (s Status) Error() string {
	return s.ErrorMsg
}

func (s Status) String() string {
	str, ok := statusName[s.Code]
	if ok {
		return str + " : " + s.ErrorMsg
	}
	return "Unknown Status"
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
}
