package kinetic

import "fmt"

func ExampleUploadAppletFile() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options
	var option = ClientOptions{
		Host: "10.29.24.55",
		Port: 8123,
		User: 1,
		Hmac: []byte("asdfasdf")}

	conn, err := NewBlockConnection(option)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	file := "not/exist/applet/javapplet.jar"
	keys, err := UploadAppletFile(conn, file, "test-applet")
	if err != nil || len(keys) <= 0 {
		fmt.Println("Upload applet file fail: ", file, err)
	}
}

func ExampleUpdateFirmware() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options
	var option = ClientOptions{
		Host: "10.29.24.55",
		Port: 8123,
		User: 1,
		Hmac: []byte("asdfasdf")}

	conn, err := NewBlockConnection(option)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	file := "not/exist/firmare/unknown-version.slod"
	err = UpdateFirmware(conn, file)
	if err != nil {
		fmt.Println("Firmware update fail: ", file, err)
	}
}
