package kinetic

import (
	"bytes"

	kproto "github.com/yongzhy/kinetic-go/proto"
)

type NonBlockConnection struct {
	service *networkService
}

// Helper function to establish non-block connection to device.
func NewNonBlockConnection(op ClientOptions) (*NonBlockConnection, error) {
	if op.Hmac == nil {
		klog.Panic("HMAC is required for ClientOptions")
	}

	service, err := newNetworkService(op)
	if err != nil {
		return nil, err
	}

	return &NonBlockConnection{service}, nil
}

func (conn *NonBlockConnection) NoOp(h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_NOOP)

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) get(key []byte, getType kproto.Command_MessageType, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(getType)
	cmd.Body = &kproto.Command_Body{
		KeyValue: &kproto.Command_KeyValue{
			Key: key,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) Get(key []byte, h *ResponseHandler) error {
	return conn.get(key, kproto.Command_GET, h)
}

func (conn *NonBlockConnection) GetNext(key []byte, h *ResponseHandler) error {
	return conn.get(key, kproto.Command_GETNEXT, h)
}

func (conn *NonBlockConnection) GetPrevious(key []byte, h *ResponseHandler) error {
	return conn.get(key, kproto.Command_GETPREVIOUS, h)
}

func (conn *NonBlockConnection) GetKeyRange(r *KeyRange, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_GETKEYRANGE)
	cmd.Body = &kproto.Command_Body{
		Range: &kproto.Command_Range{
			StartKey:          r.StartKey,
			EndKey:            r.EndKey,
			StartKeyInclusive: &r.StartKeyInclusive,
			EndKeyInclusive:   &r.EndKeyInclusive,
			MaxReturned:       &r.Max,
			Reverse:           &r.Reverse,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) GetVersion(key []byte, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_GETVERSION)
	cmd.Body = &kproto.Command_Body{
		KeyValue: &kproto.Command_KeyValue{
			Key: key,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) Flush(h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_FLUSHALLDATA)

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) Delete(entry *Record, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_DELETE)

	sync := convertSyncToProto(entry.Sync)
	//algo := convertAlgoToProto(entry.Algo)
	cmd.Body = &kproto.Command_Body{
		KeyValue: &kproto.Command_KeyValue{
			Key:             entry.Key,
			Force:           &entry.Force,
			Synchronization: &sync,
			//Algorithm:       &algo,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) Put(entry *Record, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_PUT)

	sync := convertSyncToProto(entry.Sync)
	algo := convertAlgoToProto(entry.Algo)
	cmd.Body = &kproto.Command_Body{
		KeyValue: &kproto.Command_KeyValue{
			Key:             entry.Key,
			Force:           &entry.Force,
			Synchronization: &sync,
			Algorithm:       &algo,
			Tag:             entry.Tag,
		},
	}

	return conn.service.submit(msg, cmd, entry.Value, h)
}

func (conn *NonBlockConnection) buildP2PMessage(request *P2PPushRequest) *kproto.Command_P2POperation {
	var p2pop *kproto.Command_P2POperation = nil
	if request != nil {
		p2pop = &kproto.Command_P2POperation{
			Peer: &kproto.Command_P2POperation_Peer{
				Hostname: &request.HostName,
				Port:     &request.Port,
				Tls:      &request.Tls,
			},
			Operation: make([]*kproto.Command_P2POperation_Operation, len(request.Operations)),
		}
		for k, op := range request.Operations {
			p2pop.Operation[k] = &kproto.Command_P2POperation_Operation{
				Key:     op.Key,
				Version: op.Version,
				NewKey:  nil,
				Force:   &op.Force,
				P2Pop:   conn.buildP2PMessage(op.Request),
			}
			if op.NewKey != nil && !bytes.Equal(op.NewKey, op.Key) {
				p2pop.Operation[k].NewKey = op.NewKey
			}
		}
	}
	return p2pop
}

func (conn *NonBlockConnection) P2PPush(request *P2PPushRequest, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_PEER2PEERPUSH)

	cmd.Body = &kproto.Command_Body{
		P2POperation: conn.buildP2PMessage(request),
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) GetLog(logs []LogType, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	types := make([]kproto.Command_GetLog_Type, len(logs))
	for l := range logs {
		types[l] = convertLogTypeToProto(logs[l])
	}
	cmd := newCommand(kproto.Command_GETLOG)
	cmd.Body = &kproto.Command_Body{
		GetLog: &kproto.Command_GetLog{
			Types: types,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) pinop(pin []byte, op kproto.Command_PinOperation_PinOpType, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_PINAUTH)
	msg.PinAuth = &kproto.Message_PINauth{
		Pin: pin,
	}

	cmd := newCommand(kproto.Command_PINOP)

	cmd.Body = &kproto.Command_Body{
		PinOp: &kproto.Command_PinOperation{
			PinOpType: &op,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) SecureErase(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_SECURE_ERASE_PINOP, h)
}

func (conn *NonBlockConnection) InstantErase(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_ERASE_PINOP, h)

}

func (conn *NonBlockConnection) LockDevice(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_LOCK_PINOP, h)
}

func (conn *NonBlockConnection) UnlockDevice(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_UNLOCK_PINOP, h)
}

func (conn *NonBlockConnection) UpdateFirmware(code []byte, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SETUP)

	var download bool = true
	cmd.Body = &kproto.Command_Body{
		Setup: &kproto.Command_Setup{
			FirmwareDownload: &download,
		},
	}

	return conn.service.submit(msg, cmd, code, h)
}

func (conn *NonBlockConnection) SetClusterVersion(version int64, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SETUP)

	cmd.Body = &kproto.Command_Body{
		Setup: &kproto.Command_Setup{
			NewClusterVersion: &version,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) SetLockPin(currentPin []byte, newPin []byte, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SECURITY)

	cmd.Body = &kproto.Command_Body{
		Security: &kproto.Command_Security{
			OldLockPIN: currentPin,
			NewLockPIN: newPin,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) SetErasePin(currentPin []byte, newPin []byte, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SECURITY)

	cmd.Body = &kproto.Command_Body{
		Security: &kproto.Command_Security{
			OldErasePIN: currentPin,
			NewErasePIN: newPin,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) SetACL(acls []SecurityACL, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SECURITY)

	cmd_acl := make([]*kproto.Command_Security_ACL, len(acls))
	for ka, acl := range acls {
		cmd_scope := make([]*kproto.Command_Security_ACL_Scope, len(acl.Scope))
		for ks, scope := range acl.Scope {
			cmd_permission := make([]kproto.Command_Security_ACL_Permission, len(scope.Permission))
			for kp, permission := range scope.Permission {
				cmd_permission[kp] = convertACLPermissionToProto(permission)
			}
			cmd_scope[ks] = &kproto.Command_Security_ACL_Scope{
				Offset:      &scope.Offset,
				Value:       scope.Value,
				Permission:  cmd_permission,
				TlsRequired: &scope.TlsRequired,
			}
		}
		cmd_acl_algo := convertACLAlgorithmToProto(acl.Algo)
		cmd_priority := convertPriorityToProto(acl.MaxPriority)
		cmd_acl[ka] = &kproto.Command_Security_ACL{
			Identity:      &acl.Identify,
			Key:           acl.Key,
			HmacAlgorithm: &cmd_acl_algo,
			Scope:         cmd_scope,
			MaxPriority:   &cmd_priority,
		}
	}

	cmd.Body = &kproto.Command_Body{
		Security: &kproto.Command_Security{
			Acl: cmd_acl,
		},
	}

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) MediaScan(op *MediaOperation, pri Priority, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_MEDIASCAN)

	cmd.Body = &kproto.Command_Body{
		Range: &kproto.Command_Range{
			StartKey:          op.StartKey,
			EndKey:            op.EndKey,
			StartKeyInclusive: &op.StartKeyInclusive,
			EndKeyInclusive:   &op.EndKeyInclusive,
		},
	}

	cmd_pri := convertPriorityToProto(pri)
	cmd.Header.Priority = &cmd_pri

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) MediaOptimize(op *MediaOperation, pri Priority, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_MEDIAOPTIMIZE)

	cmd.Body = &kproto.Command_Body{
		Range: &kproto.Command_Range{
			StartKey:          op.StartKey,
			EndKey:            op.EndKey,
			StartKeyInclusive: &op.StartKeyInclusive,
			EndKeyInclusive:   &op.EndKeyInclusive,
		},
	}

	cmd_pri := convertPriorityToProto(pri)
	cmd.Header.Priority = &cmd_pri

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) Listen(h *ResponseHandler) error {
	err := conn.service.listen()
	h.wait()
	return err
}

func (conn *NonBlockConnection) Close() {
	conn.service.close()
}
