package kinetic

import (
	"os"

	"github.com/Sirupsen/logrus"
	kproto "github.com/yongzhy/kinetic-go/proto"
)

// Create logger for Kinetic package
var klog = logrus.New()

func init() {
	klog.Out = os.Stdout
}

type ClientOptions struct {
	Host string
	Port int
	User int
	Hmac []byte
}

type Callback interface {
	Success()
	Failure()
}

type MessageHandler interface {
	Handle(cmd *kproto.Command, value []byte) error
	Error()
}

type Client interface {
	Nop() error
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	GetRange(startKey []byte, startKeyInclusive bool, endKey []byte, endKeyInclusive bool, reverse bool, max int32) ([][]byte, error)
}
