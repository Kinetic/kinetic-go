package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type Callback interface {
	Success(resp *kproto.Command, value []byte)
	Failure(status Status)
	Done() bool
	Status() Status
}

// Generic Callback, for Message which doesn't require data from Kinetic drive.
type GenericCallback struct {
	done   bool
	status Status
}

func (c *GenericCallback) Success(resp *kproto.Command, value []byte) {
	c.done = true
	c.status = Status{Code: OK}
	klog.Info("Callback Success")
}

func (c *GenericCallback) Failure(status Status) {
	c.done = true
	c.status = status
	klog.Info("Callback Failure")
}

func (c *GenericCallback) Done() bool {
	return c.done
}

func (c *GenericCallback) Status() Status {
	return c.status
}

// Callback for Command_GET Message
type GetCallback struct {
	GenericCallback
	Entry Record
}

func (c *GetCallback) Success(resp *kproto.Command, value []byte) {
	c.GenericCallback.Success(resp, value)
	c.Entry.Key = resp.GetBody().GetKeyValue().GetKey()
	c.Entry.Tag = resp.GetBody().GetKeyValue().GetTag()
	c.Entry.Version = resp.GetBody().GetKeyValue().GetDbVersion()
	c.Entry.Algo = convertAlgoFromProto(resp.GetBody().GetKeyValue().GetAlgorithm())

	c.Entry.Value = value
}

// Callback for Command_GETKEYRANGE Message
type GetKeyRangeCallback struct {
	GenericCallback
	Keys [][]byte
}

func (c *GetKeyRangeCallback) Success(resp *kproto.Command, value []byte) {
	c.GenericCallback.Success(resp, value)
	c.Keys = resp.GetBody().GetRange().GetKeys()
}

// Callback for Command_GETVERSION Message
type GetVersionCallback struct {
	GenericCallback
	Version []byte
}

func (c *GetVersionCallback) Success(resp *kproto.Command, value []byte) {
	c.GenericCallback.Success(resp, value)
	c.Version = resp.GetBody().GetKeyValue().GetDbVersion()
}

// Callback for Command_GETLOG Message
type GetLogCallback struct {
	GenericCallback
	Logs Log
}

func (c *GetLogCallback) Success(resp *kproto.Command, value []byte) {
	c.GenericCallback.Success(resp, value)
}
