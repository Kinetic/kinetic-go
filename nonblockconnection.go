package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type NonBlockConnection struct {
	service *networkService
}

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

func (conn *NonBlockConnection) NoOp(h *MessageHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_NOOP)

	err := conn.service.submit(msg, cmd, nil, h)
	return err
}

func (conn *NonBlockConnection) Get(key []byte, h *MessageHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(kproto.Command_GET)
	cmd.Body = &kproto.Command_Body{
		KeyValue: &kproto.Command_KeyValue{
			Key: key,
		},
	}

	err := conn.service.submit(msg, cmd, nil, h)
	return err
}

func (conn *NonBlockConnection) Run() error {
	return conn.service.listen()
}

func (conn *NonBlockConnection) Close() {
	conn.service.close()
}
