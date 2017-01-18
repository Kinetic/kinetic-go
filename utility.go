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
	"fmt"
	"io"
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

// UploadFile is the utility function to upload file to drive.
// conn is BlockConnection to drive, file is the full path to the file.
// The file may be stored into multiple object files depends on its size and input chunkSize.
// Input number of keys should equal to total number of object files on drive.
// If any chunk PUT fail, upload will stop and return status.
func UploadFile(conn *BlockConnection, file string, keys [][]byte, chunkSize int32) ([]Status, error) {
	info, err := os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Errorf("Upload fail, file %s not exist", file)
		}
		return nil, err
	}

	fileSize := info.Size()
	if fileSize <= 0 {
		return nil, fmt.Errorf("File content empty, can't upload")
	}

	if chunkSize <= 0 || chunkSize > 1024*1024 {
		return nil, fmt.Errorf("Chunk size should with range (1 -- %d)", 1024*1024)
	}

	chunks := fileSize / int64(chunkSize)
	if fileSize%int64(chunkSize) != 0 {
		chunks++
	}

	if len(keys) != int(chunks) {
		return nil, fmt.Errorf("Expect %d keys, actual %d keys", chunks, len(keys))
	}

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, chunkSize)
	var n int
	var offset, cnt int = 0, 0

	status := make([]Status, 0)

	for {
		n, err = f.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				// TODO: Should delete already PUT objects???
				return nil, err
			}
		}

		entry := Record{
			Key:   keys[cnt],
			Value: buf[:n],
			Tag:   []byte(""),
			Sync:  SyncWriteThrough,
			Algo:  AlgorithmSHA1,
			Force: true,
		}
		sts, err := conn.Put(&entry)
		status = append(status, sts)
		if err != nil || sts.Code != OK {
			klog.Errorf("Upload fail for chunk[%02d], key[%s] : %s\n", cnt, keys[cnt], sts.Error())
			// TODO: Should delete already PUT objects???
			return status, err
		}

		offset += n
		cnt++
	}

	return status, nil
}
