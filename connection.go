package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type Connection struct {
	service *networkService
}

func NewConnection(op ClientOptions) (*Connection, error) {
	if op.Hmac == nil {
		klog.Panic("HMAC is required for ClientOptions")
	}

	service, err := newNetworkService(op)
	if err != nil {
		return nil, err
	}

	return &Connection{service}, nil
}

func (conn *Connection) Nop() error {
	msg := newMessage(kproto.Message_HMACAUTH)
	cmd := newCommand(kproto.Command_NOOP)

	err := conn.service.execute(msg, cmd, nil, nil)
	return err
}

func (conn *Connection) Close() {
	conn.service.close()
}
