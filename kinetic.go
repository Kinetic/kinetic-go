package kinetic

import (
	"os"

	"github.com/Sirupsen/logrus"
)

// Create logger for Kinetic package
var klog = logrus.New()

func init() {
	klog.Out = os.Stdout
}

type ClientOptions struct {
	Host string
	Port int
	User int64
	Hmac []byte
}

// algorithm
type Algorithm int32

const (
	ALGO_INVALID_ALGORITHM Algorithm = -1
	ALGO_SHA1              Algorithm = 1
	ALGO_SHA2              Algorithm = 2
	ALGO_SHA3              Algorithm = 3
	ALGO_CRC32             Algorithm = 4
	ALGO_CRC64             Algorithm = 5
)

type Record struct {
	Key     []byte
	Value   []byte
	Version []byte
	Tag     []byte
	Algo    Algorithm
}

type KeyRange struct {
	StartKey          []byte
	EndKey            []byte
	StartKeyInclusive bool
	EndKeyInclusive   bool
	Reverse           bool
	Max               uint
}

type Client interface {
	Nop() error
	Version() error
	Put(key, value []byte, h *MessageHandler) error
	Get(key []byte, h *MessageHandler) ([]byte, error)
	GetNext() error
	GetPrevious() error
	Flush(h *MessageHandler) error
	Delete(key []byte, h *MessageHandler) error
	GetRange(r *KeyRange, h *MessageHandler) ([][]byte, error)

	SetErasePin(old, new []byte, h *MessageHandler) error
	SecureErase(pin []byte) error
	InstantErase(pin []byte) error
	SetLockPin(old, new []byte) error
	Lock(pin []byte) error
	UnLock(pin []byte) error
	GetLog() error
}
