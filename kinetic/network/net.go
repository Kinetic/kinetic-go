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

package network

import (
	"github.com/golang/protobuf/proto"	
	"encoding/binary"
	"net"
	"io"
	"errors"
	
	kproto "github.com/seagate/kinetic-go/kinetic/proto"
)

// Refactor
func SendFrom(conn net.Conn, msg *kproto.Message, length int, queue <-chan []byte) error {
	msg_bytes, err := proto.Marshal(msg)
	if err != nil { return err }
	
	header := make([]byte, 9)
	header[0] = 70 // magic
	binary.BigEndian.PutUint32(header[1:5], uint32(len(msg_bytes)))
	binary.BigEndian.PutUint32(header[5:9], uint32(length))
	
	conn.Write(header)
	conn.Write(msg_bytes)
	sent := 0
	for {
		chunk := <-queue
		ln := len(chunk)
		if ln + sent > length {
			// TODO: should shut down socket to cancel operation.
			return errors.New("Tried to send more bytes than promised.")
		}
		if chunk == nil { break }
		conn.Write(chunk)
		sent += ln
	}
	
	if sent < length {
		// TODO: should shut down socket to cancel operation.
		return errors.New("Received less bytes than promised.")
	}
	
	return nil
}

func Send(conn net.Conn, msg *kproto.Message, value []byte) error {
	msg_bytes, err := proto.Marshal(msg)
	if err != nil { return err }
	
	header := make([]byte, 9)
	header[0] = 70 // magic
	binary.BigEndian.PutUint32(header[1:5], uint32(len(msg_bytes)))
	binary.BigEndian.PutUint32(header[5:9], uint32(len(value)))
	
	conn.Write(header)
	conn.Write(msg_bytes)
	conn.Write(value)
	
	return nil
}

func Receive(conn net.Conn) (*kproto.Message, *kproto.Command, []byte, error) {
	header := make([]byte, 9)
	
	_, err := io.ReadFull(conn, header[0:])
	if err != nil { return nil, nil, nil, err }
	
	magic := header[0]
	if magic != 70 { panic("Invalid magic number!") }
	
    proto_ln := int32(binary.BigEndian.Uint32(header[1:5]))
    value_ln := int32(binary.BigEndian.Uint32(header[5:9]))
	
	proto_bytes := make([]byte, proto_ln)
	
	_, err = io.ReadFull(conn, proto_bytes)
	if err != nil { return nil, nil, nil, err }
	
	msg := &kproto.Message{}
	err = proto.Unmarshal(proto_bytes, msg)
	if err != nil { return nil, nil, nil, err }
	
	// TODO: check hmac
	
	cmd := &kproto.Command{}
	err = proto.Unmarshal(msg.CommandBytes, cmd)
	if err != nil { return nil, nil, nil, err }
	
	if value_ln > 0 {
    	value := make([]byte, value_ln)
		
		_, err = io.ReadFull(conn, value)
		if err != nil { return nil, nil, nil, err }
		
		return msg, cmd, value, nil
	} else {
		return msg, cmd, nil, nil
	}
}