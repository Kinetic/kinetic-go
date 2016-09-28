package kinetic

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

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

func UploadAppletFile(conn *BlockConnection, file, key string) ([][]byte, error) {
	info, err := os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Errorf("Upload applet fail, file %s not exist", file)
		}
		return nil, err
	}

	fileSize := info.Size()
	var chunkSize int64 = 1024 * 1024
	chunks := fileSize / chunkSize
	if fileSize%chunkSize != 0 {
		chunks++
	}
	keys := make([][]byte, chunks)

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, chunkSize)
	var n int
	var offset, cnt int = 0, 0

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
		keys[cnt] = []byte(fmt.Sprintf("%s-%010d", key, offset))

		entry := Record{
			Key:   keys[cnt],
			Value: buf[:n],
			Tag:   []byte(""),
			Sync:  SYNC_WRITETHROUGH,
			Algo:  ALGO_SHA1,
			Force: true,
		}
		status, err := conn.Put(&entry)
		if err != nil || status.Code != OK {
			klog.Errorf("Upload applet fail for chunk[%02d], key[%s] : %s\n", cnt, keys[cnt], status.Error())
			// TODO: Should delete already PUT objects???
			return nil, err
		}

		offset += n
		cnt++
	}

	return keys, nil
}
