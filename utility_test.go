package kinetic

import (
	"testing"
)

func TestUpdateFirmware(t *testing.T) {
	// file not exist, expected to fail
	file := "not/exist/firmare/unknown-version.slod"
	err := UpdateFirmware(blockConn, file)
	if err != nil {
		t.Fatal("Firmware update fail: ", file)
	}
}
