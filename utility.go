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
