package kinetic

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

type Status struct {
	Code     StatusCode
	ErrorMsg string
}

func (s Status) Error() string {
	return s.ErrorMsg
}
