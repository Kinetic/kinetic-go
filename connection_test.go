package kinetic

import (
	"os"
	"testing"
)

var (
	testConn *BlockConnection
)

const testDevice string = "10.29.24.55"

func TestMain(m *testing.M) {
	testConn = nil
	code := m.Run()
	os.Exit(code)
}

func TestHandshake(t *testing.T) {

	if testConn == nil {
		t.Skip("No Connection, skip this test")
	}
	var option = ClientOptions{
		Host: testDevice, Port: 8123,
		User: 1, Hmac: []byte("asfdasfd")}

	conn, err := NewNonBlockConnection(option)
	if err != nil {
		t.Fatal("Handshake fail")
	}

	conn.Close()
}

func TestNonBlockGet(t *testing.T) {
	var option = ClientOptions{
		Host: testDevice, Port: 8123,
		User: 1, Hmac: []byte("asfdasfd")}

	conn, err := NewBlockConnection(option)
	if err != nil {
		t.Fatal("Handshake fail")
	}

	conn.Get([]byte("object000"))
	conn.Close()
}
