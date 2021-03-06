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
	kproto "github.com/Kinetic/kinetic-go/proto"
)

// BlockConnection sends kinetic message to devices and wait for response message from device.
// For all API functions, it will only return after response from kinetic device handled.
// If no data required from kinetic device, API function will return Status and error.
// If any data required from kinetic device, the data will be one of the return values.
type BlockConnection struct {
	nbc *NonBlockConnection
}

// NewBlockConnection is helper function to establish block connection to device.
func NewBlockConnection(op ClientOptions) (*BlockConnection, error) {
	nbc, err := NewNonBlockConnection(op)
	if err != nil {
		klog.Error("Can't establish nonblocking connection")
		return nil, err
	}

	return &BlockConnection{nbc: nbc}, err
}

// NoOp does nothing but wait for drive to return response.
// On success, Status.Code will be OK
func (conn *BlockConnection) NoOp() (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.NoOp(h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

func (conn *BlockConnection) get(key []byte, getCmd kproto.Command_MessageType) (*Record, Status, error) {
	callback := &GetCallback{}
	h := NewResponseHandler(callback)

	var err error
	switch getCmd {
	case kproto.Command_GET:
		err = conn.nbc.Get(key, h)
	case kproto.Command_GETPREVIOUS:
		err = conn.nbc.GetPrevious(key, h)
	case kproto.Command_GETNEXT:
		err = conn.nbc.GetNext(key, h)
	}
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return &callback.Entry, callback.Status(), err
}

// Get gets the object from kinetic drive with key.
// On success, object Record will return and Status.Code = OK
func (conn *BlockConnection) Get(key []byte) (*Record, Status, error) {
	return conn.get(key, kproto.Command_GET)
}

// GetNext gets the next object with key after the passed in key.
// On success, object Record will return and Status.Code = OK
func (conn *BlockConnection) GetNext(key []byte) (*Record, Status, error) {
	return conn.get(key, kproto.Command_GETNEXT)
}

// GetPrevious gets the previous object with key before the passed in key.
// On success, object Record will return and Status.Code = OK
func (conn *BlockConnection) GetPrevious(key []byte) (*Record, Status, error) {
	return conn.get(key, kproto.Command_GETPREVIOUS)
}

// GetKeyRange gets list of objects' keys, which meet the criteria defined by KeyRange.
// On success, list of objects's keys returned, and Status.Code = OK
func (conn *BlockConnection) GetKeyRange(r *KeyRange) ([][]byte, Status, error) {
	callback := &GetKeyRangeCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.GetKeyRange(r, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Keys, callback.Status(), err
}

// GetVersion gets object DB version information.
// On success, version information will return and Status.Code = OK
func (conn *BlockConnection) GetVersion(key []byte) ([]byte, Status, error) {
	callback := &GetVersionCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.GetVersion(key, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Version, callback.Status(), err
}

// Flush requests kinetic device to write all cached data to persistent media.
// On success, Status.Code = OK
func (conn *BlockConnection) Flush() (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.Flush(h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// Delete deletes object from kinetic device.
// On success, Status.Code = OK
func (conn *BlockConnection) Delete(entry *Record) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.Delete(entry, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// Put store object to kinetic device.
// On success, Status.Code = OK
func (conn *BlockConnection) Put(entry *Record) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.Put(entry, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// P2PPush performs peer to peer push operation
func (conn *BlockConnection) P2PPush(request *P2PPushRequest) (*P2PPushStatus, Status, error) {
	callback := &P2PPushCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.P2PPush(request, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return &callback.P2PStatus, callback.Status(), err
}

// BatchStart starts new batch operation, all following batch PUT / DELETE share same batch ID until
// BatchEnd or BatchAbort is called.
func (conn *BlockConnection) BatchStart() (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.BatchStart(h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// BatchPut puts objects to kinetic drive, as a batch job. Batch PUT / DELETE won't expect acknowledgement
// from kinetic device. Status for batch PUT / DELETE will only available in response message for BatchEnd.
func (conn *BlockConnection) BatchPut(entry *Record) error {
	return conn.nbc.BatchPut(entry)
}

// BatchDelete delete object from kinetic drive, as a batch job. Batch PUT / DELETE won't expect acknowledgement
// from kinetic device. Status for batch PUT / DELETE will only available in response message for BatchEnd.
func (conn *BlockConnection) BatchDelete(entry *Record) error {
	return conn.nbc.BatchDelete(entry)
}

// BatchEnd commits all batch jobs. Response from kinetic device will indicate succeeded jobs sequence number, or
// the first failed job sequence number if there is a failure.
func (conn *BlockConnection) BatchEnd() (*BatchStatus, Status, error) {
	callback := &BatchEndCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.BatchEnd(h)
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return &callback.BatchStatus, callback.Status(), err
}

// BatchAbort aborts jobs in current batch operation.
func (conn *BlockConnection) BatchAbort() (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.BatchAbort(h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// GetLog gets kinetic device Log information. Can request single LogType or multiple LogType.
// On success, device Log information will return, and Status.Code = OK
func (conn *BlockConnection) GetLog(logs []LogType) (*Log, Status, error) {
	callback := &GetLogCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.GetLog(logs, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return &callback.Logs, callback.Status(), err
}

func (conn *BlockConnection) pinop(pin []byte, op kproto.Command_PinOperation_PinOpType) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)

	var err error
	switch op {
	case kproto.Command_PinOperation_SECURE_ERASE_PINOP:
		err = conn.nbc.SecureErase(pin, h)
	case kproto.Command_PinOperation_ERASE_PINOP:
		err = conn.nbc.InstantErase(pin, h)
	case kproto.Command_PinOperation_LOCK_PINOP:
		err = conn.nbc.LockDevice(pin, h)
	case kproto.Command_PinOperation_UNLOCK_PINOP:
		err = conn.nbc.UnlockDevice(pin, h)
	}
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// SecureErase request kinetic device to perform secure erase.
// SSL connection is requested to perform this operation, and the erase pin is needed.
// On success, Status.Code = OK
func (conn *BlockConnection) SecureErase(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_SECURE_ERASE_PINOP)
}

// InstantErase request kinetic device to perform instant erase.
// SSL connection is requested to perform this operation, and the erase pin is needed.
// On success, Status.Code = OK
func (conn *BlockConnection) InstantErase(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_ERASE_PINOP)

}

// LockDevice locks the kinetic device.
// SSL connection is requested to perform this operation, and the lock pin is needed.
// On success, Status.Code = OK
func (conn *BlockConnection) LockDevice(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_LOCK_PINOP)
}

// UnlockDevice unlocks the kinetic device.
// SSL connection is requested to perform this operation, and the lock pin is needed.
// On success, Status.Code = OK
func (conn *BlockConnection) UnlockDevice(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_UNLOCK_PINOP)
}

// UpdateFirmware requests to update kientic device firmware.
// Status.OK will return if firmware data received by kinetic device.
// Then drive will reboot and perform the firmware update process.
func (conn *BlockConnection) UpdateFirmware(code []byte) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.UpdateFirmware(code, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// SetClusterVersion sets the cluster version on kinetic drive.
// On success, Status.Code = OK.
func (conn *BlockConnection) SetClusterVersion(version int64) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.SetClusterVersion(version, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// SetClientClusterVersion sets the cluster version for all following message to kinetic device.
func (conn *BlockConnection) SetClientClusterVersion(version int64) {
	conn.nbc.SetClientClusterVersion(version)
}

// SetLockPin changes kinetic device lock pin. Both current pin and new pin needed.
// SSL connection is required to perform this operation.
// On success, Status.Code = OK.
func (conn *BlockConnection) SetLockPin(currentPin []byte, newPin []byte) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.SetLockPin(currentPin, newPin, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// SetErasePin changes kinetic device erase pin. Both current pin and new pin needed.
// SSL connection is required to perform this operation.
// On success, Status.Code = OK.
func (conn *BlockConnection) SetErasePin(currentPin []byte, newPin []byte) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.SetErasePin(currentPin, newPin, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// SetACL sets Permission for particular user Identity.
// On success, Status.Code = OK.
func (conn *BlockConnection) SetACL(acls []ACL) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.SetACL(acls, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// MediaScan is to check that the user data is readable, and
// if the end to end integrity is known to the device, if the
// end to end integrity field is correct.
func (conn *BlockConnection) MediaScan(op *MediaOperation, pri Priority) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.MediaScan(op, pri, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// MediaOptimize performs optimizations of the media. Things like
// defragmentation, compaction, garbage collection, compression
// could be things accomplished using the media optimize command.
func (conn *BlockConnection) MediaOptimize(op *MediaOperation, pri Priority) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.MediaOptimize(op, pri, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// SetPowerLevel sets device power level
func (conn *BlockConnection) SetPowerLevel(p PowerLevel) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.SetPowerLevel(p, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

// Close the connection to kientic device
func (conn *BlockConnection) Close() {
	conn.nbc.Close()
}
