package kinetic

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	proto "github.com/golang/protobuf/proto"
	kproto "github.com/yongzhy/kinetic-go/proto"
)

var (
	networkTimeout = 20 * time.Second
)

func newMessage(t kproto.Message_AuthType) *kproto.Message {
	msg := &kproto.Message{
		AuthType: t.Enum(),
	}
	if msg.GetAuthType() == kproto.Message_HMACAUTH {
		msg.HmacAuth = &kproto.Message_HMACauth{}
	}

	return msg
}

func newCommand(t kproto.Command_MessageType) *kproto.Command {
	return &kproto.Command{
		Header: &kproto.Command_Header{
			MessageType: t.Enum(),
		},
	}
}

type networkService struct {
	conn   net.Conn
	seq    int64                      // Operation sequence ID
	connId int64                      // current conection ID
	option ClientOptions              // current connection operation
	hmap   map[int64]*ResponseHandler // Message handler map
	fatal  bool                       // Network has fatal failure
}

func newNetworkService(op ClientOptions) (*networkService, error) {
	target := fmt.Sprintf("%s:%d", op.Host, op.Port)
	conn, err := net.DialTimeout("tcp", target, networkTimeout)
	if err != nil {
		klog.Panic("Can't establish connection to ", op.Host)
		return nil, err
	}

	ns := &networkService{
		conn:   conn,
		seq:    0,
		connId: 0,
		option: op,
		hmap:   make(map[int64]*ResponseHandler),
		fatal:  false,
	}

	_, _, _, err = ns.receive()
	if err != nil {
		klog.Error("Can't establish connection to %s", op.Host)
		return nil, err
	}

	return ns, nil
}

// When client network service has error, call error handling
// from all Messagehandler current in Queue.
func (ns *networkService) clientError(s Status, mh *ResponseHandler) {
	for ack, h := range ns.hmap {
		if h.callback != nil {
			h.callback.Failure(s)
		}
		delete(ns.hmap, ack)
	}
	if mh != nil && mh.callback != nil {
		mh.callback.Failure(s)
	}
}

func (ns *networkService) listen() error {
	if ns.fatal {
		return errors.New("Network service has fatal error")
	}

	if len(ns.hmap) == 0 {
		return nil
	}

	msg, cmd, value, err := ns.receive()
	if err != nil {
		klog.Error("Network Service listen error")
		return err
	}

	klog.Info("Kinetic response received ", cmd.GetHeader().GetMessageType().String())

	if msg.GetAuthType() == kproto.Message_UNSOLICITEDSTATUS {
		if cmd.GetHeader() != nil {
			*(cmd.GetHeader().AckSequence) = -1
		}
	}

	ack := cmd.GetHeader().GetAckSequence()
	h, ok := ns.hmap[ack]
	if ok == false {
		klog.Warn("Couldn't find a handler for acksequence ", ack)
		return nil
	}

	(*h).Handle(cmd, value)

	delete(ns.hmap, ack)

	return nil
}

func (ns *networkService) submit(msg *kproto.Message, cmd *kproto.Command, value []byte, h *ResponseHandler) error {
	if ns.fatal {
		return errors.New("Network service has fatal error")
	}
	cmd.GetHeader().ConnectionID = &ns.connId
	cmd.GetHeader().Sequence = &ns.seq
	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		klog.Error("Error marshl Kinetic Command")
		s := Status{CLIENT_INTERNAL_ERROR, "Error marshl Kinetic Command"}
		ns.clientError(s, h)
		return err
	}
	msg.CommandBytes = cmdBytes[:]

	if msg.GetAuthType() == kproto.Message_HMACAUTH {
		msg.GetHmacAuth().Identity = &ns.option.User
		msg.GetHmacAuth().Hmac = compute_hmac(msg.CommandBytes, ns.option.Hmac)
	}

	err = ns.send(msg, value)
	if err != nil {
		return err
	}

	klog.Info("Kinetic message send ", cmd.GetHeader().GetMessageType().String())

	if h != nil {
		ns.hmap[ns.seq] = h
		klog.Info("Insert handler for ACK ", ns.seq)
	}

	// update sequence number
	// TODO: Need mutex protection here
	ns.seq++

	return nil
}

func (ns *networkService) send(msg *kproto.Message, value []byte) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		s := Status{CLIENT_INTERNAL_ERROR, "Error marshl Kinetic Message"}
		ns.clientError(s, nil)
		return err
	}

	// Set timeout for send packet
	ns.conn.SetWriteDeadline(time.Now().Add(networkTimeout))

	// Construct message header 9 bytes
	header := make([]byte, 9)
	header[0] = 'F' // Magic number
	binary.BigEndian.PutUint32(header[1:5], uint32(len(msgBytes)))
	binary.BigEndian.PutUint32(header[5:9], uint32(len(value)))

	packet := append(header, msgBytes...)
	if value != nil && len(value) > 0 {
		packet = append(packet, value...)
	}

	_, err = ns.conn.Write(packet)
	if err != nil {
		klog.Error("Network I/O write error")
		s := Status{CLIENT_IO_ERROR, "Network I/O write error"}
		ns.clientError(s, nil)
		ns.fatal = true
		return err
	}

	return nil
}

func (ns *networkService) receive() (*kproto.Message, *kproto.Command, []byte, error) {
	// Set timeout for receive packet
	ns.conn.SetReadDeadline(time.Now().Add(networkTimeout))

	header := make([]byte, 9)

	_, err := io.ReadFull(ns.conn, header[0:])
	if err != nil {
		klog.Error("Network I/O read error")
		s := Status{CLIENT_IO_ERROR, "Network I/O read error"}
		ns.clientError(s, nil)
		ns.fatal = true
		return nil, nil, nil, err
	}

	magic := header[0]
	if magic != 'F' {
		klog.Error("Network I/O read error Header wrong magic")
		s := Status{CLIENT_IO_ERROR, "Network I/O read error Header wrong magic"}
		ns.clientError(s, nil)
		ns.fatal = true
		return nil, nil, nil, errors.New("Network I/O read error Header wrong magic")
	}

	protoLen := int(binary.BigEndian.Uint32(header[1:5]))
	valueLen := int(binary.BigEndian.Uint32(header[5:9]))

	protoBuf := make([]byte, protoLen)
	_, err = io.ReadFull(ns.conn, protoBuf)
	if err != nil {
		klog.Error("Network I/O read error receive Kinetic Header")
		s := Status{CLIENT_IO_ERROR, "Network I/O read error receive Kinetic Header"}
		ns.clientError(s, nil)
		ns.fatal = true
		return nil, nil, nil, err
	}

	msg := &kproto.Message{}
	err = proto.Unmarshal(protoBuf, msg)
	if err != nil {
		klog.Error("Network I/O read error receive Kinetic Header")
		s := Status{CLIENT_IO_ERROR, "Network I/O read error reaceive Kinetic Message"}
		ns.clientError(s, nil)
		ns.fatal = true
		return nil, nil, nil, err
	}

	if msg.GetAuthType() == kproto.Message_HMACAUTH && validate_hmac(msg, ns.option.Hmac) == false {
		klog.Error("Response HMAC mismatch")
		s := Status{CLIENT_RESPONSE_HMAC_VERIFICATION_ERROR, "Response HMAC mismatch"}
		ns.clientError(s, nil)
		return nil, nil, nil, err
	}

	cmd := &kproto.Command{}
	err = proto.Unmarshal(msg.CommandBytes, cmd)
	if err != nil {
		klog.Error("Network I/O read error parsing Kinetic Command")
		s := Status{CLIENT_IO_ERROR, "Network I/O read error parsing Kinetic Command"}
		ns.clientError(s, nil)
		ns.fatal = true
		return nil, nil, nil, err
	}

	if cmd.Header != nil && cmd.Header.ConnectionID != nil {
		ns.connId = cmd.GetHeader().GetConnectionID()
	}

	if valueLen > 0 {
		valueBuf := make([]byte, valueLen)
		_, err = io.ReadFull(ns.conn, valueBuf)
		if err != nil {
			klog.Error("Network I/O read error parsing Kinetic Value")
			s := Status{CLIENT_IO_ERROR, "Network I/O read error parsing Kinetic Value"}
			ns.clientError(s, nil)
			ns.fatal = true
			return nil, nil, nil, err
		}

		return msg, cmd, valueBuf, nil
	}

	return msg, cmd, nil, nil
}

func (ns *networkService) close() {
	ns.conn.Close()
	klog.Infof("Connection to %s closed", ns.option.Host)
}
