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
	"bytes"
	"testing"

	kproto "github.com/Kinetic/kinetic-go/proto"
	proto "github.com/golang/protobuf/proto"
)

func TestHmacEmptyMessage(t *testing.T) {
	expected := []byte{0xa7, 0x7a, 0x6a, 0xda, 0x5c, 0xe6,
		0x7c, 0xf7, 0xae, 0xe4, 0x8a, 0x79, 0xd4, 0x86,
		0x6b, 0xb2, 0x71, 0x24, 0x18, 0x15}
	hmac := computeHmac(nil, []byte("asdfasdf"))

	if !bytes.Equal(expected, hmac) {
		t.Fatal("HMAC for empty Command Failed")
	}
}

func TestHmacSimpleMessage(t *testing.T) {
	expected := []byte{0x40, 0x5F, 0x94, 0x9F, 0xC3, 0x50,
		0xDC, 0x0B, 0x6A, 0x5A, 0x9D, 0x27, 0xA3, 0xCA,
		0x44, 0x58, 0x9D, 0xB3, 0x4A, 0xCD}
	cmd := kproto.Command{nil, nil, nil, nil}
	code := kproto.Command_Status_SUCCESS
	cmd.Status = &kproto.Command_Status{&code, nil, nil, nil}
	cmdBytes, _ := proto.Marshal(&cmd)
	hmac := computeHmac(cmdBytes, []byte("asdfasdf"))
	if !bytes.Equal(expected, hmac) {
		t.Fatal("HMAC for simple Command Failed")
	}
}
