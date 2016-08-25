package kinetic

type Connection struct {
	service *networkService
}

func NewConnection(op ClientOptions) (*Connection, error) {
	if op.Hmac == nil {
		klog.Panic("HMAC is required for ClientOptions")
	}

	service, err := newNetworkService(op)
	if err != nil {
		return nil, err
	}

	return &Connection{service}, nil
}

func (conn *Connection) Nop() error {
	return nil
}

func (conn *Connection) Close() {
	conn.service.close()
}
