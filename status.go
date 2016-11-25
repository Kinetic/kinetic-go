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
	"strconv"

	kproto "github.com/Kinetic/kinetic-go/proto"
)

// StatusCode for kinetic message.
// Including status code get from device, or client internal error code.
type StatusCode int32

// StatusCode code value
const (
	RemoteNotAttempted                 StatusCode = iota
	OK                                 StatusCode = iota
	ClientIOError                      StatusCode = iota
	ClientShutdown                     StatusCode = iota
	ClientInternalError                StatusCode = iota
	ClientResponseHMACError            StatusCode = iota
	RemoteHMACError                    StatusCode = iota
	RemoteNotAuthorized                StatusCode = iota
	RemoteClusterVersionMismatch       StatusCode = iota
	RemoteInvalidRequest               StatusCode = iota
	RemoteInternalError                StatusCode = iota
	RemoteHeaderRequired               StatusCode = iota
	RemoteNotFound                     StatusCode = iota
	RemoteVersionMismatch              StatusCode = iota
	RemoteServiceBusy                  StatusCode = iota
	RemoteExpired                      StatusCode = iota
	RemoteDataError                    StatusCode = iota
	RemotePermDataError                StatusCode = iota
	RemoteConnectionError              StatusCode = iota
	RemoteNoSpace                      StatusCode = iota
	RemoteNoSuchHMACAlgorithm          StatusCode = iota
	RemoteOtherError                   StatusCode = iota
	ProtocolErrorResponseNoAckSequence StatusCode = iota
	RemoteNestedOperationErrors        StatusCode = iota
	RemoteDeviceLocked                 StatusCode = iota
	RemoteDeviceAlreadyUnlocked        StatusCode = iota
	RemoteConnectionTerminated         StatusCode = iota
	RemoteInvalidBatch                 StatusCode = iota
	RemoteInvalidExecute               StatusCode = iota
	RemoteExecuteComplete              StatusCode = iota
	RemoteHibernate                    StatusCode = iota
	RemoteShutdown                     StatusCode = iota
)

var statusName = map[StatusCode]string{
	RemoteNotAttempted:                 "REMOTE_NOT_ATTEMPTED",
	OK:                                 "OK",
	ClientIOError:                      "CLIENT_IO_ERROR",
	ClientShutdown:                     "CLIENT_SHUTDOWN",
	ClientInternalError:                "CLIENT_INTERNAL_ERROR",
	ClientResponseHMACError:            "CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR",
	RemoteHMACError:                    "REMOTE_HMAC_ERROR",
	RemoteNotAuthorized:                "REMOTE_NOT_AUTHORIZED",
	RemoteClusterVersionMismatch:       "REMOTE_CLUSTER_VERSION_MISMATCH",
	RemoteInvalidRequest:               "REMOTE_INVALID_REQUEST",
	RemoteInternalError:                "REMOTE_INTERNAL_ERROR",
	RemoteHeaderRequired:               "REMOTE_HEADER_REQUIRED",
	RemoteNotFound:                     "REMOTE_NOT_FOUND",
	RemoteVersionMismatch:              "REMOTE_VERSION_MISMATCH",
	RemoteServiceBusy:                  "REMOTE_SERVICE_BUSY",
	RemoteExpired:                      "REMOTE_EXPIRED",
	RemoteDataError:                    "REMOTE_DATA_ERROR",
	RemotePermDataError:                "REMOTE_PERM_DATA_ERROR",
	RemoteConnectionError:              "REMOTE_CONNECTION_ERROR",
	RemoteNoSpace:                      "REMOTE_NO_SPACE",
	RemoteNoSuchHMACAlgorithm:          "REMOTE_NO_SUCH_HMAC_ALGORITHM",
	RemoteOtherError:                   "REMOTE_OTHER_ERROR",
	ProtocolErrorResponseNoAckSequence: "PROTOCOL_ERROR_RESPONSE_NO_ACKSEQUENCE ",
	RemoteNestedOperationErrors:        "REMOTE_NESTED_OPERATION_ERRORS",
	RemoteDeviceLocked:                 "REMOTE_DEVICE_LOCKED",
	RemoteDeviceAlreadyUnlocked:        "REMOTE_DEVICE_ALREADY_UNLOCKED",
	RemoteConnectionTerminated:         "REMOTE_CONNECTION_TERMINATED",
	RemoteInvalidBatch:                 "REMOTE_INVALID_BATCH",
	RemoteInvalidExecute:               "REMOTE_INVALID_EXECUTE",
	RemoteExecuteComplete:              "REMOTE_EXECUTE_COMPLETE",
	RemoteHibernate:                    "REMOTE_HIBERNATE",
	RemoteShutdown:                     "REMOTE_SHUTDOWN",
}

// Status for each kinetic message.
// Code is the status code and ErrorMsg is the detail message
type Status struct {
	Code                   StatusCode
	ErrorMsg               string
	ExpectedClusterVersion int64
}

// Error returns the detail status message if Status.Code != OK
func (s Status) Error() string {
	return s.ErrorMsg
}

func (s Status) String() string {
	ret := "Unknown Status"
	str, ok := statusName[s.Code]
	if ok {
		ret = str + " : " + s.ErrorMsg
		if s.Code == RemoteClusterVersionMismatch {
			ret = ret + ", Expected cluster version =" + strconv.Itoa(int(s.ExpectedClusterVersion))
		}
	}
	return ret
}

func convertStatusCodeToProto(s StatusCode) kproto.Command_Status_StatusCode {
	ret := kproto.Command_Status_INVALID_STATUS_CODE
	switch s {
	case RemoteNotAttempted:
		ret = kproto.Command_Status_NOT_ATTEMPTED
	case OK:
		ret = kproto.Command_Status_SUCCESS
	case RemoteHMACError:
		ret = kproto.Command_Status_HMAC_FAILURE
	case RemoteNotAuthorized:
		ret = kproto.Command_Status_NOT_AUTHORIZED
	case RemoteClusterVersionMismatch:
		ret = kproto.Command_Status_VERSION_FAILURE
	case RemoteInternalError:
		ret = kproto.Command_Status_INTERNAL_ERROR
	case RemoteHeaderRequired:
		ret = kproto.Command_Status_HEADER_REQUIRED
	case RemoteNotFound:
		ret = kproto.Command_Status_NOT_FOUND
	case RemoteVersionMismatch:
		ret = kproto.Command_Status_VERSION_MISMATCH
	case RemoteServiceBusy:
		ret = kproto.Command_Status_SERVICE_BUSY
	case RemoteExpired:
		ret = kproto.Command_Status_EXPIRED
	case RemoteDataError:
		ret = kproto.Command_Status_DATA_ERROR
	case RemotePermDataError:
		ret = kproto.Command_Status_PERM_DATA_ERROR
	case RemoteConnectionError:
		ret = kproto.Command_Status_REMOTE_CONNECTION_ERROR
	case RemoteNoSpace:
		ret = kproto.Command_Status_NO_SPACE
	case RemoteNoSuchHMACAlgorithm:
		ret = kproto.Command_Status_NO_SUCH_HMAC_ALGORITHM
	case RemoteInvalidRequest:
		ret = kproto.Command_Status_INVALID_REQUEST
	case RemoteNestedOperationErrors:
		ret = kproto.Command_Status_NESTED_OPERATION_ERRORS
	case RemoteDeviceLocked:
		ret = kproto.Command_Status_DEVICE_LOCKED
	case RemoteDeviceAlreadyUnlocked:
		ret = kproto.Command_Status_DEVICE_ALREADY_UNLOCKED
	case RemoteConnectionTerminated:
		ret = kproto.Command_Status_CONNECTION_TERMINATED
	case RemoteInvalidBatch:
		ret = kproto.Command_Status_INVALID_BATCH
	case RemoteHibernate:
		ret = kproto.Command_Status_HIBERNATE
	case RemoteShutdown:
		ret = kproto.Command_Status_SHUTDOWN
	}
	return ret
}

func convertStatusCodeFromProto(s kproto.Command_Status_StatusCode) StatusCode {
	ret := RemoteOtherError
	switch s {
	case kproto.Command_Status_NOT_ATTEMPTED:
		ret = RemoteNotAttempted
	case kproto.Command_Status_SUCCESS:
		ret = OK
	case kproto.Command_Status_HMAC_FAILURE:
		ret = RemoteHMACError
	case kproto.Command_Status_NOT_AUTHORIZED:
		ret = RemoteNotAuthorized
	case kproto.Command_Status_VERSION_FAILURE:
		ret = RemoteClusterVersionMismatch
	case kproto.Command_Status_INTERNAL_ERROR:
		ret = RemoteInternalError
	case kproto.Command_Status_HEADER_REQUIRED:
		ret = RemoteHeaderRequired
	case kproto.Command_Status_NOT_FOUND:
		ret = RemoteNotFound
	case kproto.Command_Status_VERSION_MISMATCH:
		ret = RemoteVersionMismatch
	case kproto.Command_Status_SERVICE_BUSY:
		ret = RemoteServiceBusy
	case kproto.Command_Status_EXPIRED:
		ret = RemoteExpired
	case kproto.Command_Status_DATA_ERROR:
		ret = RemoteDataError
	case kproto.Command_Status_PERM_DATA_ERROR:
		ret = RemotePermDataError
	case kproto.Command_Status_REMOTE_CONNECTION_ERROR:
		ret = RemoteConnectionError
	case kproto.Command_Status_NO_SPACE:
		ret = RemoteNoSpace
	case kproto.Command_Status_NO_SUCH_HMAC_ALGORITHM:
		ret = RemoteNoSuchHMACAlgorithm
	case kproto.Command_Status_INVALID_REQUEST:
		ret = RemoteInvalidRequest
	case kproto.Command_Status_NESTED_OPERATION_ERRORS:
		ret = RemoteNestedOperationErrors
	case kproto.Command_Status_DEVICE_LOCKED:
		ret = RemoteDeviceLocked
	case kproto.Command_Status_DEVICE_ALREADY_UNLOCKED:
		ret = RemoteDeviceAlreadyUnlocked
	case kproto.Command_Status_CONNECTION_TERMINATED:
		ret = RemoteConnectionTerminated
	case kproto.Command_Status_INVALID_BATCH:
		ret = RemoteInvalidBatch
	case kproto.Command_Status_HIBERNATE:
		ret = RemoteHibernate
	case kproto.Command_Status_SHUTDOWN:
		ret = RemoteShutdown
	}
	return ret
}

func getStatusFromProto(cmd *kproto.Command) Status {
	code := convertStatusCodeFromProto(cmd.GetStatus().GetCode())
	msg := cmd.GetStatus().GetStatusMessage()
	version := cmd.GetHeader().GetClusterVersion()

	return Status{code, msg, version}
}
