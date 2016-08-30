package kinetic

//kproto "github.com/yongzhy/kinetic-go/proto"

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

func (conn *BlockConnection) NoOp() error {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	if h == nil {
		klog.Error("Message Handler for NoOp Failure")
	}
	if conn == nil {
		klog.Error("Connection nil")
	} else if conn.nbc == nil {
		klog.Error("Nonblock Connection nil")
	}
	conn.nbc.NoOp(h)

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return nil
}

func (conn *BlockConnection) Get(key []byte) (Record, error) {
	callback := &GetCallback{}
	h := NewMessageHandler(callback)

	err := conn.nbc.Get(key, h)
	if err != nil {
		return Record{}, err
	}

	for callback.Done() == false {
		conn.nbc.Run()
	}

	return callback.Record(), nil
}

func (conn *BlockConnection) Close() {
	conn.nbc.Close()
}
