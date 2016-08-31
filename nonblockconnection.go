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

func (conn *NonBlockConnection) get(key []byte, getType kproto.Command_MessageType, h *MessageHandler) error {
	msg := newMessage(kproto.Message_HMACAUTH)

	cmd := newCommand(getType)
	cmd.Body = &kproto.Command_Body{
		KeyValue: &kproto.Command_KeyValue{
			Key: key,
		},
	}

	err := conn.service.submit(msg, cmd, nil, h)
	return err
}

func (conn *NonBlockConnection) Get(key []byte, h *MessageHandler) error {
	return conn.get(key, kproto.Command_GET, h)
}

func (conn *NonBlockConnection) GetNext(key []byte, h *MessageHandler) error {
	return conn.get(key, kproto.Command_GETNEXT, h)
}

func (conn *NonBlockConnection) GetPrevious(key []byte, h *MessageHandler) error {
	return conn.get(key, kproto.Command_GETPREVIOUS, h)
}

func (conn *NonBlockConnection) GetKeyRange(r *KeyRange, h *MessageHandler) error {
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

	err := conn.service.submit(msg, cmd, nil, h)
	return err
}

func (conn *NonBlockConnection) Delete(entry *Record, h *MessageHandler) error {
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

	err := conn.service.submit(msg, cmd, nil, h)
	return err
}

func (conn *NonBlockConnection) Put(entry *Record, h *MessageHandler) error {
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

	err := conn.service.submit(msg, cmd, entry.Value, h)
	return err
}

func (conn *NonBlockConnection) Run() error {
	return conn.service.listen()
}

func (conn *NonBlockConnection) Close() {
	conn.service.close()
}
