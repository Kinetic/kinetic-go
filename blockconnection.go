package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

// BlockConnection is block version of connection to kinetic drvice.
// For all API fucntions, it will only return after get response from kinetic drvice.
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

func (conn *BlockConnection) P2PPush(request *P2PPushRequest) ([]Status, Status, error) {
	callback := &P2PPushCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.P2PPush(request, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Statuses, callback.Status(), err
}

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

func (conn *BlockConnection) SecureErase(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_SECURE_ERASE_PINOP)
}

func (conn *BlockConnection) InstantErase(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_ERASE_PINOP)

}

func (conn *BlockConnection) LockDevice(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_LOCK_PINOP)
}

func (conn *BlockConnection) UnlockDevice(pin []byte) (Status, error) {
	return conn.pinop(pin, kproto.Command_PinOperation_UNLOCK_PINOP)
}

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

func (conn *BlockConnection) SetClientClusterVersion(version int64) {
	conn.nbc.SetClientClusterVersion(version)
}

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

func (conn *BlockConnection) SetACL(acls []SecurityACL) (Status, error) {
	callback := &GenericCallback{}
	h := NewResponseHandler(callback)
	err := conn.nbc.SetACL(acls, h)
	if err != nil {
		return callback.Status(), err
	}

	err = conn.nbc.Listen(h)

	return callback.Status(), err
}

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

func (conn *BlockConnection) Close() {
	conn.nbc.Close()
}
