/**
 * Copyright 2013-2016 Seagate Technology LLC.
 *
 * This Source Code Form is subject to the terms of the Mozilla
 * Public License, v. 2.0. If a copy of the MPL was not
 * distributed with this file, You can obtain one at
 * https://mozilla.org/MP:/2.0/.
 *
 * This program is distributed in the hope that it will be useful,
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public
 * License for more details.
 *
 * See www.openkinetic.org for more project information
 */

package kinetic

import (
	"sync"

	kproto "github.com/Kinetic/kinetic-go/proto"
)

// ResponseHandler is the handler for XXXXX_RESPONSE message from drive.
// For each operation, a unique ResponseHandler is required
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
				h.callback.Failure(cmd, getStatusFromProto(cmd))
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
		h.callback.Failure(nil, s)
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
// For each operation, a unique ResponseHandler is required
func NewResponseHandler(call Callback) *ResponseHandler {
	h := &ResponseHandler{callback: call, done: false, cond: sync.NewCond(&sync.Mutex{})}
	return h
}
