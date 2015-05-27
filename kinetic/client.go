// The MIT License (MIT)
//
// Copyright (c) 2015 Seagate Technology
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// @author = ["Ignacio Corderi"]

package kinetic

import (
	"github.com/golang/protobuf/proto"	
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"net"
	"fmt"
	
	"github.com/seagate/kinetic-go/kinetic/network"
	kproto "github.com/seagate/kinetic-go/kinetic/proto"
)

// Default credentials
var (
	USER_ID = int64(1)
    CLIENT_SECRET = []byte("asdfasdf")
)

func calculate_hmac(secret []byte, bytes []byte) []byte {
	mac := hmac.New(sha1.New, secret)
    
	ln := make([]byte, 4)
    binary.BigEndian.PutUint32(ln, uint32(len(bytes)))
	
    mac.Write(ln)
    mac.Write(bytes)

	return mac.Sum(nil)
} 

type RemoteError struct {
	status kproto.Command_Status	
}

func (e RemoteError) Error() string {
	return fmt.Sprintf("%v: %v", e.status.Code, *e.status.StatusMessage)
}

type Client interface {
	Put (key []byte, value []byte) ((<-chan error), error)
	
	Close()
}

type PendingOperation struct {
	sequence int64
	receiver chan error
}

type NetworkClient struct {
	connectionId int64 
	userId       int64
	secret       []byte
	sequence     int64
	conn         net.Conn
	closed       bool
	error        error	
	notifier     chan<- PendingOperation 
}

func Connect(target string) (Client, error) {
	conn, err := net.Dial("tcp", target)
	if err != nil { return nil, err }
	// hanshake
	_, cmd, _, err := network.Receive(conn)
	if err != nil { return nil, err }
	
	ch := make(chan PendingOperation)
	
	c := &NetworkClient { connectionId: *cmd.Header.ConnectionID,
		                 userId: USER_ID,
						 secret: CLIENT_SECRET,
						 sequence: 1,
						 conn: conn, 
						 closed: false,
						 error: nil,
						 notifier: ch }
				
	go c.listen(ch)						 
	return c, nil						 
} 

func (self *NetworkClient) listen(notifications <-chan PendingOperation) {
	pending := make(map[int64]PendingOperation) // pendings
	for {
		_, cmd, _, err := network.Receive(self.conn)
		if err != nil { 
			if !self.closed { 
				self.error = err
				// TODO: try closing socket
			}
			break
		}
		
		var response error
		if *cmd.Status.Code != kproto.Command_Status_SUCCESS {
			response = RemoteError { status: *cmd.Status }
		}
		
		// Notify caller
		// Seems more complicated than it should, but we are optimizing 
		// for when we receive in order
		for {
			op := <-notifications
			// Chances are, it's in order
			if op.sequence == *cmd.Header.AckSequence {
				op.receiver <- response // TODO: send back the actual response
				break
			} else {				
				// Either we missed it or it hasnt arrived yet.
				pending[op.sequence] = op				 
				op, ok := pending[*cmd.Header.AckSequence]
				if ok { // this is the case where we missed it
					op.receiver <- response
					delete(pending, op.sequence)
					break
				}
			}			
		}		
	}
	
	// Notify all pendings that we are closed for business
}

// Client implementation

func(self *NetworkClient) Put(key []byte, value []byte) ((<-chan error), error) {	
	cmd := &kproto.Command {
			Header: &kproto.Command_Header {
				ConnectionID: proto.Int64(self.connectionId),
				Sequence: proto.Int64(self.sequence),
				MessageType: kproto.Command_PUT.Enum(),
			},
			Body: &kproto.Command_Body {
				KeyValue: &kproto.Command_KeyValue {
					Key: key,
					Algorithm: kproto.Command_SHA1.Enum(),
					Tag: make([]byte, 0),
					Synchronization: kproto.Command_WRITEBACK.Enum(),
				},
			},
		}
		
	cmd_bytes, err := proto.Marshal(cmd)		
	if err != nil { return nil, err }
	
	msg := &kproto.Message {
			AuthType: kproto.Message_HMACAUTH.Enum(),
			HmacAuth: &kproto.Message_HMACauth {
				Identity: proto.Int64(self.userId),
				Hmac: calculate_hmac(self.secret, cmd_bytes),
			},
			CommandBytes: cmd_bytes,
		}
	
	network.Send(self.conn, msg, value)	
			
	rx := make(chan error, 1)		
	pending := PendingOperation { sequence: self.sequence, receiver: rx }		
			
	self.notifier <- pending			
			
	self.sequence += 1
	
	return rx, nil
}

func(self *NetworkClient) Close() {
	self.closed = true
	self.conn.Close()
}