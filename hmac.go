package kinetic

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"

	kproto "github.com/yongzhy/kinetic-go/proto"
)

func compute_hmac(data []byte, key []byte) []byte {
	mac := hmac.New(sha1.New, key)

	if data != nil && len(data) > 0 {
		ln := make([]byte, 4)
		binary.BigEndian.PutUint32(ln, uint32(len(data)))

		mac.Write(ln)
		mac.Write(data)
	}

	return mac.Sum(nil)
}

func validate_hmac(mesg *kproto.Message, key []byte) bool {
	if mesg != nil {
		real := compute_hmac(mesg.GetCommandBytes(), key)

		if mesg.GetHmacAuth() != nil {
			expect := mesg.GetHmacAuth().GetHmac()
			if hmac.Equal(real, expect) {
				return true
			}
		}
	}
	return false
}
