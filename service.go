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
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	kproto "github.com/Kinetic/kinetic-go/proto"
	proto "github.com/golang/protobuf/proto"
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
	rxMu           sync.Mutex
	txMu           sync.Mutex
	mapMu          sync.Mutex
	conn           net.Conn
	clusterVersion int64                      // Cluster version
	seq            int64                      // Operation sequence ID
	connID         int64                      // current connection ID
	option         ClientOptions              // current connection operation
	hmap           map[int64]*ResponseHandler // Message handler map
	fatal          bool                       // Network has fatal failure
	fatalError     error                      // Network fatal error details
	device         Log                        // Store device information from handshake package
}

func newNetworkService(op ClientOptions) (*networkService, error) {
	var conn net.Conn
	var err error
	if op.Timeout > 0 {
		networkTimeout = time.Duration(op.Timeout) * time.Millisecond
	}

	target := fmt.Sprintf("%s:%d", op.Host, op.Port)
	if op.UseSSL {
		// TODO: Need to enable verify certification later
		config := tls.Config{InsecureSkipVerify: true}
		d := &net.Dialer{Timeout: networkTimeout}
		conn, err = tls.DialWithDialer(d, "tcp", target, &config)
	} else {
		conn, err = net.DialTimeout("tcp", target, networkTimeout)
	}

	if err != nil {
		klog.Error("Can't establish connection to ", op.Host, err)
		return nil, err
	}

	ns := &networkService{
		conn:           conn,
		clusterVersion: 0,
		seq:            0,
		connID:         -1,
		option:         op,
		hmap:           make(map[int64]*ResponseHandler),
		fatal:          false,
		fatalError:     nil,
	}

	ns.rxMu.Lock()
	// Do the handshake.
	// Device Configuration and Limits from handshake will be stored in networkService.device
	_, _, _, err = ns.receive()
	ns.rxMu.Unlock()

	if err != nil {
		klog.Error("Can't establish connection to %s", op.Host)
		return nil, err
	}

	klog.Debugf("Connected to %s:%d", op.Host, op.Port)
	klog.Debugf("\tVendor: %s", ns.device.Configuration.Vendor)
	klog.Debugf("\tModel: %s", ns.device.Configuration.Model)
	klog.Debugf("\tWorldWideName: %s", ns.device.Configuration.WorldWideName)
	klog.Debugf("\tSerial Number: %s", ns.device.Configuration.SerialNumber)
	klog.Debugf("\tFirmware Version: %s", ns.device.Configuration.Version)
	klog.Debugf("\tKinetic Protocol Version: %s", ns.device.Configuration.ProtocolVersion)
	klog.Debugf("\tPort: %d", ns.device.Configuration.Port)
	klog.Debugf("\tTlsPort: %d", ns.device.Configuration.TLSPort)
	klog.Debugf("\tCurrentPowerLevel : %s", ns.device.Configuration.CurrentPowerLevel.String())

	return ns, nil
}

// When client network service has error, call error handling
// from all Messagehandler current in Queue.
func (ns *networkService) clientError(s Status, mh *ResponseHandler) {
	ns.mapMu.Lock()
	for ack, h := range ns.hmap {
		h.fail(s)
		delete(ns.hmap, ack)
	}
	ns.mapMu.Unlock()

	if mh != nil {
		mh.fail(s)
	}
}

func (ns *networkService) listen() error {
	if ns.fatal {
		return errors.New("Can't listen, network service has fatal error: " + ns.fatalError.Error())
	}

	ns.mapMu.Lock()
	if len(ns.hmap) == 0 {
		ns.mapMu.Unlock()
		return nil
	}
	ns.mapMu.Unlock()

	ns.rxMu.Lock()
	msg, cmd, value, err := ns.receive()
	ns.rxMu.Unlock()
	if err != nil {
		klog.Error("Network Service listen error")
		return err
	}

	if cmd.GetHeader() != nil {
		klog.Debug("Kinetic response received ", cmd.GetHeader().GetMessageType().String(),
			", AckSeq = ", cmd.GetHeader().GetAckSequence(),
			", Code = ", cmd.GetStatus().GetCode())
	} else if msg.GetAuthType() == kproto.Message_UNSOLICITEDSTATUS {
		klog.Debug("Kinetic UNSOLICITEDSTATUS : ",
			"Code = ", cmd.GetStatus().GetCode(),
			", StatusMessage = ", cmd.GetStatus().GetStatusMessage())
	}

	// For UNSOLICITEDSTATUS, command may not have Header or AckSequence, set the ack to -1 so
	// no ResponseHandler will be found from hmap table.
	var ack int64 = -1
	if cmd.Header != nil && cmd.Header.AckSequence != nil {
		ack = cmd.GetHeader().GetAckSequence()
	}

	ns.mapMu.Lock()
	h, ok := ns.hmap[ack]
	ns.mapMu.Unlock()
	if ok == false {
		// It's high chance this is an UNSOLICITEDSTATUS message, display the Status.
		klog.Errorf("Couldn't find a handler for acksequence %d, status=%s", ack, getStatusFromProto(cmd).String())
		// This is an unexpected packet. Each listen() call expect remove one ResponseHandler from hmap.
		// So need to fire another listen() to make sure ResponseHandler in hmap got chance to exit.
		// Either by receive correct packet, or network read failure.
		go ns.listen()
		return nil
	}

	h.handle(cmd, value)

	ns.mapMu.Lock()
	delete(ns.hmap, ack)
	ns.mapMu.Unlock()

	return nil
}

// submit will send the message to kinetic device, insert ResponseHandler for this message sequence number.
// ResponseHandler can be nil if the message no require for Ack, eg batch PUT / DELETE.
func (ns *networkService) submit(msg *kproto.Message, cmd *kproto.Command, value []byte, h *ResponseHandler) error {
	if ns.fatal {
		return errors.New("Can't submit, network service has fatal error: " + ns.fatalError.Error())
	}

	ns.txMu.Lock()

	cmd.GetHeader().ConnectionID = &ns.connID
	cmd.GetHeader().Sequence = &ns.seq
	cmd.GetHeader().ClusterVersion = &ns.clusterVersion

	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		klog.Error("Error marshl Kinetic Command")
		s := Status{Code: ClientInternalError, ErrorMsg: "Error marshl Kinetic Command"}
		ns.clientError(s, h)
		return err
	}
	msg.CommandBytes = cmdBytes[:]

	if msg.GetAuthType() == kproto.Message_HMACAUTH {
		msg.GetHmacAuth().Identity = &ns.option.User
		msg.GetHmacAuth().Hmac = computeHmac(msg.CommandBytes, ns.option.Hmac)
	}

	klog.Debug("Kinetic message send ", cmd.GetHeader().GetMessageType().String(), " Seq = ", ns.seq)

	err = ns.send(msg, value)

	if err != nil {
		return err
	}

	if h != nil {
		ns.mapMu.Lock()
		ns.hmap[ns.seq] = h
		ns.mapMu.Unlock()
	}

	ns.seq++
	ns.txMu.Unlock()

	return nil
}

func (ns *networkService) send(msg *kproto.Message, value []byte) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		s := Status{Code: ClientInternalError, ErrorMsg: "Error marshl Kinetic Message"}
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
		klog.Error("Network I/O write error, " + err.Error())
		s := Status{Code: ClientIOError, ErrorMsg: "Network I/O write error, " + err.Error()}
		ns.clientError(s, nil)
		ns.fatal = true
		ns.fatalError = err
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
		klog.Error("Network I/O read error, " + err.Error())
		s := Status{Code: ClientIOError, ErrorMsg: "Network I/O read error, " + err.Error()}
		ns.clientError(s, nil)
		ns.fatal = true
		ns.fatalError = err
		return nil, nil, nil, err
	}

	magic := header[0]
	if magic != 'F' {
		klog.Error("Network I/O read error Header wrong magic")
		s := Status{Code: ClientIOError, ErrorMsg: "Network I/O read error Header wrong magic"}
		ns.clientError(s, nil)
		ns.fatal = true
		ns.fatalError = errors.New("Wrong magic number")
		return nil, nil, nil, errors.New("Network I/O read error Header wrong magic")
	}

	protoLen := int(binary.BigEndian.Uint32(header[1:5]))
	valueLen := int(binary.BigEndian.Uint32(header[5:9]))

	protoBuf := make([]byte, protoLen)
	_, err = io.ReadFull(ns.conn, protoBuf)
	if err != nil {
		klog.Error("Network I/O read error receive Kinetic Header, " + err.Error())
		s := Status{Code: ClientIOError, ErrorMsg: "Network I/O read error receive Kinetic Header, " + err.Error()}
		ns.clientError(s, nil)
		ns.fatal = true
		ns.fatalError = err
		return nil, nil, nil, err
	}

	msg := &kproto.Message{}
	err = proto.Unmarshal(protoBuf, msg)
	if err != nil {
		klog.Error("Network I/O read error receive Kinetic Header, " + err.Error())
		s := Status{Code: ClientIOError, ErrorMsg: "Network I/O read error reaceive Kinetic Message, " + err.Error()}
		ns.clientError(s, nil)
		ns.fatal = true
		ns.fatalError = err
		return nil, nil, nil, err
	}

	if msg.GetAuthType() == kproto.Message_HMACAUTH && validateHmac(msg, ns.option.Hmac) == false {
		klog.Error("Response HMAC mismatch")
		s := Status{Code: ClientResponseHMACError, ErrorMsg: "Response HMAC mismatch"}
		ns.clientError(s, nil)
		return nil, nil, nil, err
	}

	cmd := &kproto.Command{}
	err = proto.Unmarshal(msg.CommandBytes, cmd)
	if err != nil {
		klog.Error("Network I/O read error parsing Kinetic Command, " + err.Error())
		s := Status{Code: ClientIOError, ErrorMsg: "Network I/O read error parsing Kinetic Command, " + err.Error()}
		ns.clientError(s, nil)
		ns.fatal = true
		ns.fatalError = err
		return nil, nil, nil, err
	}

	if cmd.Header != nil && cmd.Header.ConnectionID != nil {
		if ns.connID < 0 {
			// This is handshake packet
			ns.device = getLogFromProto(cmd)

			// Only update client cluster version during Handshake
			if cmd.Header.ClusterVersion != nil {
				ns.clusterVersion = cmd.GetHeader().GetClusterVersion()
			}
		}
		ns.connID = cmd.GetHeader().GetConnectionID()
	}

	if valueLen > 0 {
		valueBuf := make([]byte, valueLen)
		_, err = io.ReadFull(ns.conn, valueBuf)
		if err != nil {
			klog.Error("Network I/O read error parsing Kinetic Value, " + err.Error())
			s := Status{Code: ClientIOError, ErrorMsg: "Network I/O read error parsing Kinetic Value, " + err.Error()}
			ns.clientError(s, nil)
			ns.fatal = true
			ns.fatalError = err
			return nil, nil, nil, err
		}

		return msg, cmd, valueBuf, nil
	}

	return msg, cmd, nil, nil
}

func (ns *networkService) close() {
	ns.conn.Close()
	klog.Debugf("Connection to %s closed", ns.option.Host)
}
