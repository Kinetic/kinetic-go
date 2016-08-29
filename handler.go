package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type MessageHandler struct {
	callback Callback
}

func (h *MessageHandler) Handle(cmd *kproto.Command, value []byte) error {
	if h.callback != nil {
		if cmd.Status != nil && cmd.Status.Code != nil {
			if cmd.GetStatus().GetCode() == kproto.Command_Status_SUCCESS {
				h.callback.Success(cmd, value)
			} else {
				var status = Status{}
				h.callback.Failure(&status)
			}
		}

	}
	return nil
}

func (h *MessageHandler) SetCallback(call Callback) {
	h.callback = call
}

func NewMessageHandler(call Callback) *MessageHandler {
	h := &MessageHandler{callback: call}
	return h
}
