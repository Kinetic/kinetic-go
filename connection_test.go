package kinetic

import (
	"testing"
)

func TestHandshake(t *testing.T) {
	var option = ClientOptions{
		Host: "10.29.24.55", Port: 8123,
		User: 1, Hmac: []byte("asfdasfd")}

	conn, err := NewConnection(option)
	if err != nil {
		t.Fatal("Handshake fail")
	}

	conn.Close()
}
