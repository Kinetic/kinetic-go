package kinetic

import (
	//"fmt"
	//"io"
	"io/ioutil"
	"os"
)

// UpdateFirmware is the utility function to update drive firmware.
// conn is BlockConnection to drive, and file is the full path to the firmware file.
func UpdateFirmware(conn *BlockConnection, file string) error {
	_, err := os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Errorf("Update firmware fail, file %s not exist", file)
		}
		return err
	}

	data, err := ioutil.ReadFile(file)
	if err != nil {
		klog.Errorf("Update firmware fail, file %s can't read", file)
		return err
	}

	status, err := conn.UpdateFirmware(data)
	if err != nil || status.Code != OK {
		klog.Errorf("Update firmware fail : %s\n", status.Error())
	}

	return err
}
