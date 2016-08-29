package kinetic

//kproto "github.com/yongzhy/kinetic-go/proto"

type BlockConnection struct {
	nbc *NonBlockConnection
}

func NewBlockConnection(op ClientOptions) (*BlockConnection, error) {
	nbc, err := NewNonBlockConnection(op)
	if err != nil {
		return nil, err
	}

	return &BlockConnection{nbc: nbc}, err
}

func (conn *BlockConnection) NoOp() error {
	callback := &GenericCallback{}
	h := NewMessageHandler(callback)
	conn.nbc.Nop(h)

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

	for i := 0; i < 1000; i++ {
		if callback.Done() == false {
			conn.nbc.Run()
		}
	}
	//for callback.Done() == false {
	//		conn.nbc.Run()
	//}

	return callback.Record(), nil
}

func (conn *BlockConnection) Close() {
	conn.nbc.Close()
}
