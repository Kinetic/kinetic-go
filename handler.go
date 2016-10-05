package kinetic

import (
	"sync"

	kproto "github.com/yongzhy/kinetic-go/proto"
)

// ResponseHandler is the handler for XXXXX_RESPONSE message from drive.
// For each operation, a unique ResponseHandler is requried
type ResponseHandler struct {
	callback Callback
	done     bool
	cond     *sync.Cond
}

func (h *ResponseHandler) handle(cmd *kproto.Command, value []byte) error {
	if h.callback != nil {
		if cmd.Status != nil && cmd.Status.Code != nil {
			if cmd.GetStatus().GetCode() == kproto.Command_Status_SUCCESS {
				h.callback.Success(cmd, value)
			} else {
				h.callback.Failure(getStatusFromProto(cmd))
			}
		} else {
			klog.Warn("Other status received")
			klog.Info("%v", cmd)
		}

	}
	h.cond.L.Lock()
	h.done = true
	h.cond.Signal()
	h.cond.L.Unlock()
	return nil
}

func (h *ResponseHandler) fail(s Status) {
	if h.callback != nil {
		h.callback.Failure(s)
	}
	h.cond.L.Lock()
	h.done = true
	h.cond.Signal()
	h.cond.L.Unlock()
}

func (h *ResponseHandler) wait() {
	h.cond.L.Lock()
	if h.done == false {
		h.cond.Wait()
	}
	h.cond.L.Unlock()
}

// NewResponseHandler is helper function to build a ResponseHandler with call as the Callback.
// For each operation, a unique ResponseHandler is requried
func NewResponseHandler(call Callback) *ResponseHandler {
	h := &ResponseHandler{callback: call, done: false, cond: sync.NewCond(&sync.Mutex{})}
	return h
}
