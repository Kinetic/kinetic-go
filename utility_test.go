package kinetic

import (
	"testing"
)

func TestUploadAppletFile(t *testing.T) {
	// file not exist, expected to fail
	file := "not/exist/applet/javapplet.jar"
	keys, err := UploadAppletFile(blockConn, file, "test-applet")
	if err != nil || len(keys) <= 0 {
		t.Fatal("Upload applet file fail: ", file)
	}
}

func TestUpdateFirmware(t *testing.T) {
	// file not exist, expected to fail
	file := "not/exist/firmare/unknown-version.slod"
	err := UpdateFirmware(blockConn, file)
	if err != nil {
		t.Fatal("Firmware update fail: ", file)
	}
}
