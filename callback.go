package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type Callback interface {
	Success(resp *kproto.Command, value []byte)
	Failure(status *Status)
	Done() bool
	Status() Status
}

type GenericCallback struct {
	done   bool
	status Status
}

func (c *GenericCallback) Success(resp *kproto.Command, value []byte) {
	c.done = true
	c.status = Status{Code: OK}
	klog.Info("Callback Success")
}

func (c *GenericCallback) Failure(status *Status) {
	c.done = true
	c.status = *status
	klog.Info("Callback Failure")
}

func (c *GenericCallback) Done() bool {
	return c.done
}

func (c *GenericCallback) Status() Status {
	return c.status
}

type GetCallback struct {
	GenericCallback
	record Record
}

func (c *GetCallback) Success(resp *kproto.Command, value []byte) {
	c.GenericCallback.Success(resp, value)
	c.record.Key = resp.GetBody().GetKeyValue().GetKey()
	c.record.Value = value
	klog.Info("Get Operation Success")
	klog.Info("%v", c.record)
}

func (c *GetCallback) Record() Record {
	return c.record
}
