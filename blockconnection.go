package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type BlockConnection struct {
	nbc *NonBlockConnection
}

func NewBlockConnection(op ClientOptions) (*BlockConnection, error) {
	nbc, err := NewNonBlockConnection(op)
	if err != nil {
		klog.Error("Can't establish nonblocking connection")
		return nil, err
	}

	return &BlockConnection{nbc: nbc}, err
}

func (conn *BlockConnection) NoOp() (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.NoOp(h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) get(key []byte, getCmd kproto.Command_MessageType) (Record, Status, error) {
	callback := &GetCallback{}
	h := NewMessageHandler(callback)

	var err error = nil
	switch getCmd {
	case kproto.Command_GET:
		err = conn.nbc.Get(key, h)
	case kproto.Command_GETPREVIOUS:
		err = conn.nbc.GetPrevious(key, h)
	case kproto.Command_GETNEXT:
		err = conn.nbc.GetNext(key, h)
	}
	if err != nil {
		return Record{}, callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Entry, callback.Status(), nil
}

func (conn *BlockConnection) Get(key []byte) (Record, Status, error) {
	return conn.get(key, kproto.Command_GET)
}

func (conn *BlockConnection) GetNext(key []byte) (Record, Status, error) {
	return conn.get(key, kproto.Command_GETNEXT)
}

func (conn *BlockConnection) GetPrevious(key []byte) (Record, Status, error) {
	return conn.get(key, kproto.Command_GETPREVIOUS)
}

func (conn *BlockConnection) GetKeyRange(r *KeyRange) ([][]byte, Status, error) {
	callback := &GetKeyRangeCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.GetKeyRange(r, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Keys, callback.Status(), nil
}

func (conn *BlockConnection) GetVersion(key []byte) ([]byte, Status, error) {
	callback := &GetVersionCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.GetVersion(r, h)
	if err != nil {
		return nil, callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Version, callback.Status(), nil
}

func (conn *BlockConnection) Flush() (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.Flush(h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) Delete(entry *Record) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.Delete(entry, h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) Put(entry *Record) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.Put(entry, h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) pinop(pin []byte, op kproto.Command_PinOperation_PinOpType) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)

	var err error = nil
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

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
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
	h := NewMessageHandler(callback)
	err := conn.nbc.UpdateFirmware(code, h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) SetClusterVersion(version int64) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.SetClusterVersion(version, h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) SetLockPin(currentPin []byte, newPin []byte) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.SetLockPin(currentPin, newPin, h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) SetErasePin(currentPin []byte, newPin []byte) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	err := conn.nbc.SetErasePin(currentPin, newPin, h)
	if err != nil {
		return callback.Status(), err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) Close() {
	conn.nbc.Close()
}
