package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type Callback interface {
	Failure()
	Done() bool
}

type MessageHandler interface {
	Handle(cmd *kproto.Command, value []byte) error
	Error()
	SetCallback(callback *Callback)
}

type SimpleCallback struct {
	done bool
}

func (c *SimpleCallback) Failure() {
	c.done = true
}

func (c *SimpleCallback) Done() bool {
	return c.done
}
