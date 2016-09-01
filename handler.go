package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type ResponseHandler struct {
	callback Callback
}

func (h *ResponseHandler) Handle(cmd *kproto.Command, value []byte) error {
	klog.Info("Message handler called")
	if h.callback != nil {
		if cmd.Status != nil && cmd.Status.Code != nil {
			if cmd.GetStatus().GetCode() == kproto.Command_Status_SUCCESS {
				h.callback.Success(cmd, value)
			} else {
				h.callback.Failure(getStatusFromProto(cmd))
			}
		} else {
			klog.Info("Other status received")
			klog.Info("%v", cmd)
		}

	}
	return nil
}

func (h *ResponseHandler) Error(s Status) {
	if h.callback != nil {
		h.callback.Failure(s)
	}
}

func (h *ResponseHandler) SetCallback(call Callback) {
	h.callback = call
}

func NewResponseHandler(call Callback) *ResponseHandler {
	h := &ResponseHandler{callback: call}
	return h
}
