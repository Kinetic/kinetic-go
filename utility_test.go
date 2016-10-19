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

import "fmt"

func ExampleUpdateFirmware() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options
	var option = ClientOptions{
		Host: "127.0.0.1",
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
