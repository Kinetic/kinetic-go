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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"

	kproto "github.com/Kinetic/kinetic-go/proto"
)

func computeHmac(data []byte, key []byte) []byte {
	mac := hmac.New(sha1.New, key)

	if data != nil && len(data) > 0 {
		ln := make([]byte, 4)
		binary.BigEndian.PutUint32(ln, uint32(len(data)))

		mac.Write(ln)
		mac.Write(data)
	}

	return mac.Sum(nil)
}

func validateHmac(mesg *kproto.Message, key []byte) bool {
	if mesg != nil {
		real := computeHmac(mesg.GetCommandBytes(), key)

		if mesg.GetHmacAuth() != nil {
			expect := mesg.GetHmacAuth().GetHmac()
			if hmac.Equal(real, expect) {
				return true
			}
		}
	}
	return false
}
