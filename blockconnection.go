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
	conn.nbc.NoOp(h)

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
	conn.nbc.GetKeyRange(r, h)

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Keys, callback.Status(), nil
}

func (conn *BlockConnection) Delete(entry *Record) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	conn.nbc.Delete(entry, h)

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) Put(entry *Record) (Status, error) {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	conn.nbc.Put(entry, h)

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Status(), nil
}

func (conn *BlockConnection) Close() {
	conn.nbc.Close()
}
