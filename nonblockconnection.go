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
	"sync"

	kproto "github.com/Kinetic/kinetic-go/proto"
)

// NonBlockConnection send kinetic message to devices and doesn't wait for
// response message from device.
type NonBlockConnection struct {
	service    *networkService
	batchID    uint32 // Current batch Operation ID
	batchCount int32  // Current batch operation count
	batchMu    sync.Mutex
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

	return &NonBlockConnection{service: service, batchID: 0, batchCount: 0}, nil
}

// NoOp does nothing but wait for drive to return response.
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

// Get gets the object from kinetic drive with key.
func (conn *NonBlockConnection) Get(key []byte, h *ResponseHandler) error {
	return conn.get(key, kproto.Command_GET, h)
}

// GetNext gets the next object with key after the passed in key.
func (conn *NonBlockConnection) GetNext(key []byte, h *ResponseHandler) error {
	return conn.get(key, kproto.Command_GETNEXT, h)
}

// GetPrevious gets the previous object with key before the passed in key.
func (conn *NonBlockConnection) GetPrevious(key []byte, h *ResponseHandler) error {
	return conn.get(key, kproto.Command_GETPREVIOUS, h)
}

// GetKeyRange gets list of objects' keys, which meet the criteria defined by KeyRange.
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

// GetVersion gets object DB version information.
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

// Flush requests kinetic device to write all cached data to persistent media.
func (conn *NonBlockConnection) Flush(h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_FLUSHALLDATA)

	return conn.service.submit(msg, cmd, nil, h)
}

func (conn *NonBlockConnection) delete(entry *Record, batch bool, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_DELETE)

	// Bathc operation, batchID needed
	if batch {
		cmd.Header.BatchID = &conn.batchID
	}

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

// Delete deletes object from kinetic device.
func (conn *NonBlockConnection) Delete(entry *Record, h *ResponseHandler) error {
	// Normal DELETE operation, not batch operation.
	return conn.delete(entry, false, h)
}

func (conn *NonBlockConnection) put(entry *Record, batch bool, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_PUT)

	// Bathc operation, batchID needed
	if batch {
		cmd.Header.BatchID = &conn.batchID
	}

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

// Put store object to kinetic device.
func (conn *NonBlockConnection) Put(entry *Record, h *ResponseHandler) error {
	// Normal PUT operation, not batch operation
	return conn.put(entry, false, h)
}

func (conn *NonBlockConnection) buildP2PMessage(request *P2PPushRequest) *kproto.Command_P2POperation {
	var p2pop *kproto.Command_P2POperation
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

// P2Push
func (conn *NonBlockConnection) P2PPush(request *P2PPushRequest, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_PEER2PEERPUSH)

	cmd.Body = &kproto.Command_Body{
		P2POperation: conn.buildP2PMessage(request),
	}

	return conn.service.submit(msg, cmd, nil, h)
}

// BatchStart starts new batch operation, all following batch PUT / DELETE share same batch ID until
// BatchEnd or BatchAbort is called.
func (conn *NonBlockConnection) BatchStart(h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_START_BATCH)

	// TODO: Need to confirm can start new batch if current one not end / abort yet???
	conn.batchMu.Lock()
	conn.batchID++
	conn.batchCount = 0 // Reset
	conn.batchMu.Unlock()
	cmd.Header.BatchID = &conn.batchID
	return conn.service.submit(msg, cmd, nil, h)
}

// BatchPut puts objects to kinetic drive, as a batch job. Batch PUT / DELETE won't expect acknowledgement
// from kinetic device. Status for batch PUT / DELETE will only availabe in response message for BatchEnd.
func (conn *NonBlockConnection) BatchPut(entry *Record) error {
	// Batch operation PUT
	conn.batchMu.Lock()
	conn.batchCount++
	conn.batchMu.Unlock()
	return conn.put(entry, true, nil)
}

// BatchDelete delete object from kinetic drive, as a batch job. Batch PUT / DELETE won't expect acknowledgement
// from kinetic device. Status for batch PUT / DELETE will only availabe in response message for BatchEnd.
func (conn *NonBlockConnection) BatchDelete(entry *Record) error {
	// Batch operation DELETE
	conn.batchMu.Lock()
	conn.batchCount++
	conn.batchMu.Unlock()
	return conn.delete(entry, true, nil)
}

// BatchEnd commits all batch jobs. Response from kinetic device will indicate succeeded jobs sequence number, or
// the first failed job sequence number if there is a failure.
func (conn *NonBlockConnection) BatchEnd(h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_END_BATCH)

	cmd.Header.BatchID = &conn.batchID
	cmd.Body = &kproto.Command_Body{
		Batch: &kproto.Command_Batch{
			Count: &conn.batchCount,
		},
	}
	return conn.service.submit(msg, cmd, nil, h)
}

// BatchAbort aborts jobs in current batch operation.
func (conn *NonBlockConnection) BatchAbort(h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_ABORT_BATCH)

	cmd.Header.BatchID = &conn.batchID
	return conn.service.submit(msg, cmd, nil, h)
}

// GetLog gets kinetic device Log information. Can request single LogType or multiple LogType.
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

// SecureErase request kinetic device to perform secure erase.
// SSL connection is requested to perform this operation, and the erase pin is needed.
func (conn *NonBlockConnection) SecureErase(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_SECURE_ERASE_PINOP, h)
}

// InstantErase request kinetic device to perform instant erase.
// SSL connection is requested to perform this operation, and the erase pin is needed.
func (conn *NonBlockConnection) InstantErase(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_ERASE_PINOP, h)

}

// LockDevice locks the kinetic device.
// SSL connection is requested to perform this operation, and the lock pin is needed.
func (conn *NonBlockConnection) LockDevice(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_LOCK_PINOP, h)
}

// UnlockDevice unlocks the kinetic device.
// SSL connection is requested to perform this operation, and the lock pin is needed.
func (conn *NonBlockConnection) UnlockDevice(pin []byte, h *ResponseHandler) error {
	return conn.pinop(pin, kproto.Command_PinOperation_UNLOCK_PINOP, h)
}

// UpdateFirmware requests to update kientic device firmware.
// Then drive will reboot and perform the firmware update process.
func (conn *NonBlockConnection) UpdateFirmware(code []byte, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SETUP)

	var download = true
	cmd.Body = &kproto.Command_Body{
		Setup: &kproto.Command_Setup{
			FirmwareDownload: &download,
		},
	}

	return conn.service.submit(msg, cmd, code, h)
}

// SetClusterVersion sets the cluster version on kinetic drive.
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

// SetClientClusterVersion sets the cluster version for all following message to kinetic device.
func (conn *NonBlockConnection) SetClientClusterVersion(version int64) {
	conn.service.clusterVersion = version
}

// SetLockPin changes kinetic device lock pin. Both current pin and new pin needed.
// SSL connection is required to perform this operation.
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

// SetErasePin changes kinetic device erase pin. Both current pin and new pin needed.
// SSL connection is required to perform this operation.
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

// SetACL sets Permission for particular user Identify.
func (conn *NonBlockConnection) SetACL(acls []ACL, h *ResponseHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_SECURITY)

	cmdACL := make([]*kproto.Command_Security_ACL, len(acls))
	for ka, acl := range acls {
		cmdScope := make([]*kproto.Command_Security_ACL_Scope, len(acl.Scopes))
		for ks, scope := range acl.Scopes {
			cmdPermission := make([]kproto.Command_Security_ACL_Permission, len(scope.Permissions))
			for kp, permission := range scope.Permissions {
				cmdPermission[kp] = convertACLPermissionToProto(permission)
			}
			cmdScope[ks] = &kproto.Command_Security_ACL_Scope{
				Offset:      &scope.Offset,
				Value:       scope.Value,
				Permission:  cmdPermission,
				TlsRequired: &scope.TlsRequired,
			}
		}
		cmdAlgo := convertACLAlgorithmToProto(acl.Algo)
		cmdPriority := convertPriorityToProto(acl.MaxPriority)
		cmdACL[ka] = &kproto.Command_Security_ACL{
			Identity:      &acl.Identify,
			Key:           acl.Key,
			HmacAlgorithm: &cmdAlgo,
			Scope:         cmdScope,
			MaxPriority:   &cmdPriority,
		}
	}

	cmd.Body = &kproto.Command_Body{
		Security: &kproto.Command_Security{
			Acl: cmdACL,
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

	p := convertPriorityToProto(pri)
	cmd.Header.Priority = &p

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

	p := convertPriorityToProto(pri)
	cmd.Header.Priority = &p

	return conn.service.submit(msg, cmd, nil, h)
}

// Listen waits and read response message from device, then call ResponseHandler
// in queue to process received message.
func (conn *NonBlockConnection) Listen(h *ResponseHandler) error {
	err := conn.service.listen()
	h.wait()
	return err
}

// Close the connection to kientic device
func (conn *NonBlockConnection) Close() {
	conn.service.close()
}
