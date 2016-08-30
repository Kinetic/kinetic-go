package kinetic

import (
	"os"
	"testing"
)

var (
	blockConn *BlockConnection = nil
)

var option = ClientOptions{
	Host: "10.29.24.55",
	Port: 8123,
	User: 1,
	Hmac: []byte("asdfasdf")}

func TestMain(m *testing.M) {
	blockConn, _ = NewBlockConnection(option)
	if blockConn != nil {
		code := m.Run()
		blockConn.Close()
		os.Exit(code)
	} else {
		os.Exit(-1)
	}
}

func TestNonBlockNoOp(t *testing.T) {
	blockConn.NoOp()
}

func TestNonBlockGet(t *testing.T) {
	blockConn.Get([]byte("object000"))
}
