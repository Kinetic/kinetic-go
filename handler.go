package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type Callback interface {
	Success()
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

func (c *SimpleCallback) Success() {
	c.done = true
}

func (c *SimpleCallback) Failure() {
	c.done = true
}

func (c *SimpleCallback) Done() bool {
	return c.done
}

type SimpleHandler struct {
	callback *Callback
}

func (h *SimpleHandler) Handle(cmd *kproto.Command, value []byte) error {
	if h.callback != nil {
		if cmd.Status != nil && cmd.Status.Code != nil {
			if cmd.GetStatus().GetCode() == kproto.Command_Status_SUCCESS {
				(*h.callback).Success()
			} else {
				(*h.callback).Failure()
			}
		}

	}
	return nil
}

func (h *SimpleHandler) Error() {

}

func (h *SimpleHandler) SetCallback(call *Callback) {
	h.callback = call
}
