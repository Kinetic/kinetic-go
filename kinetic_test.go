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
	"bytes"
	"fmt"
)

func ExampleBlockConnection_putGetDelete() {
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

	// PUT
	pentry := Record{
		Key:   []byte("Test Object"),
		Value: []byte("Test Object Data"),
		Sync:  SYNC_WRITETHROUGH,
		Algo:  ALGO_SHA1,
		Tag:   []byte(""),
		Force: true,
	}
	status, err := conn.Put(&pentry)
	if err != nil || status.Code != OK {
		fmt.Println("Blocking Put Failure")
	}

	// GET back the object
	gentry, status, err := conn.Get(pentry.Key)
	if err != nil || status.Code != OK {
		fmt.Println("Blocking Get Failure")
	}

	// Verify the object Key and Value
	if !bytes.Equal(pentry.Key, gentry.Key) {
		fmt.Printf("Key Mismatch: [%s] vs [%s]\n", pentry.Key, gentry.Key)
	}
	if !bytes.Equal(pentry.Value, gentry.Value) {
		fmt.Printf("Value Mismatch: [%s] vs [%s]\n", pentry.Value, gentry.Value)
	}

	// DELETE the object
	dentry := Record{
		Key:   pentry.Key,
		Sync:  pentry.Sync,
		Force: true,
	}
	status, err = conn.Delete(&dentry)
	if err != nil || status.Code != OK {
		fmt.Println("Blocking Delete Failure")
	}
}

func ExampleBlockConnection_ssl() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options, use SSL connection
	var option = ClientOptions{
		Host:   "10.29.24.55",
		Port:   8443,
		User:   1,
		Hmac:   []byte("asdfasdf"),
		UseSSL: true,
	}

	conn, err := NewBlockConnection(option)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
}

func ExampleNonBlockConnection_putGetDelete() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options
	var option = ClientOptions{
		Host: "10.29.24.55",
		Port: 8123,
		User: 1,
		Hmac: []byte("asdfasdf")}

	conn, err := NewNonBlockConnection(option)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// PUT
	pentry := Record{
		Key:   []byte("Test Object"),
		Value: []byte("Test Object Data"),
		Sync:  SYNC_WRITETHROUGH,
		Algo:  ALGO_SHA1,
		Tag:   []byte(""),
		Force: true,
	}
	// Each Nonblock operation require specific Callback and ResponseHandler
	// For operation doesn't require data from Kinetic drive, GenericCallback will enough.
	pcallback := &GenericCallback{}
	ph := NewResponseHandler(pcallback)
	err = conn.Put(&pentry, ph)
	if err != nil {
		fmt.Println("NonBlocking Put Failure")
	}
	conn.Listen(ph)

	// GET back the object, GET operation need to process data from drive, so use GetCallBack
	gcallback := &GetCallback{}
	gh := NewResponseHandler(gcallback)
	err = conn.Get(pentry.Key, gh)
	if err != nil {
		fmt.Println("NonBlocking Get Failure")
	}
	conn.Listen(gh)
	gentry := gcallback.Entry

	// Verify the object Key and Value
	if !bytes.Equal(pentry.Key, gentry.Key) {
		fmt.Printf("Key Mismatch: [%s] vs [%s]\n", pentry.Key, gentry.Key)
	}
	if !bytes.Equal(pentry.Value, gentry.Value) {
		fmt.Printf("Value Mismatch: [%s] vs [%s]\n", pentry.Value, gentry.Value)
	}

	// DELETE the object, DELETE doesn't require data from drive, use GenericCallback
	dcallback := &GenericCallback{}
	dh := NewResponseHandler(dcallback)
	dentry := Record{
		Key:   pentry.Key,
		Sync:  pentry.Sync,
		Force: true,
	}
	err = conn.Delete(&dentry, dh)
	if err != nil {
		fmt.Println("NonBlocking Delete Failure")
	}
	conn.Listen(dh)
}

func ExampleNonBlockConnection_multiplePut() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options
	var option = ClientOptions{
		Host: "10.29.24.55",
		Port: 8123,
		User: 1,
		Hmac: []byte("asdfasdf")}

	conn, err := NewNonBlockConnection(option)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	done := make(chan bool)

	prefix := []byte("TestObject")

	// PUT
	// 1st round: main routin PUT object, start new go routine to wait for operation done
	for id := 1; id <= 100; id++ {
		key := []byte(fmt.Sprintf("%s-%05d", prefix, id))
		v := bytes.Repeat(key, id)
		if len(v) > 1024*1024 {
			v = v[:1024*1024]
		}
		pentry := Record{
			Key:   key,
			Value: v,
			Sync:  SYNC_WRITETHROUGH,
			Algo:  ALGO_SHA1,
			Tag:   []byte(""),
			Force: true,
		}
		pcallback := &GenericCallback{}
		ph := NewResponseHandler(pcallback)
		err = conn.Put(&pentry, ph)
		if err != nil {
			fmt.Println("NonBlocking Put Failure")
		}

		go func() {
			conn.Listen(ph)
			done <- true
		}()
	}

	// PUT
	// 2nd round, start new go routin for each PUT object and wait for operation done
	for id := 101; id <= 200; id++ {
		go func(id int, done chan bool) {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, id))
			v := bytes.Repeat(key, id)
			if len(v) > 1024*1024 {
				v = v[:1024*1024]
			}
			pentry := Record{
				Key:   key,
				Value: v,
				Sync:  SYNC_WRITETHROUGH,
				Algo:  ALGO_SHA1,
				Tag:   []byte(""),
				Force: true,
			}
			pcallback := &GenericCallback{}
			ph := NewResponseHandler(pcallback)
			err = conn.Put(&pentry, ph)
			if err != nil {
				fmt.Println("NonBlocking Put Failure")
			}

			conn.Listen(ph)
			done <- true
		}(id, done)
	}

	// Total 200 go routine started, wait for all to finish
	for id := 1; id <= 200; id++ {
		<-done
	}
}

func ExampleBlockConnection_SetACL() {
	// Set the log leverl to debug
	SetLogLevel(LogLevelDebug)

	// Client options
	var option = ClientOptions{
		Host:   "10.29.24.55",
		Port:   8443, // Must be SSL connection here
		User:   1,
		Hmac:   []byte("asdfasdf"),
		UseSSL: true, // Set ACL must use SSL connection
	}

	conn, err := NewBlockConnection(option)
	if err != nil {
		panic(err)
	}

	perms := []ACLPermission{
		ACL_PERMISSION_GETLOG,
	}
	scope := []ACLScope{
		ACLScope{
			Permissions: perms,
		},
	}
	acls := []ACL{
		ACL{
			Identify: 100,
			Key:      []byte("asdfasdf"),
			Algo:     ACL_ALGORITHM_HMACSHA1,
			Scopes:   scope,
		},
	}

	status, err := conn.SetACL(acls)
	if err != nil || status.Code != OK {
		fmt.Println("SetACL failure: ", err, status)
	}

	// Close the SET ACL connection
	conn.Close()

	// Next, do the verifiation on the SET ACL
	// Client options
	option = ClientOptions{
		Host: "10.29.24.55",
		Port: 8123,
		User: 100,
		Hmac: []byte("asdfasdf")}

	conn, err = NewBlockConnection(option)
	if err != nil {
		panic(err)
	}

	logs := []LogType{
		LOG_UTILIZATIONS,
		LOG_TEMPERATURES,
		LOG_CAPACITIES,
		LOG_CONFIGURATION,
		LOG_STATISTICS,
		LOG_MESSAGES,
		LOG_LIMITS,
	}

	_, status, err = conn.GetLog(logs)
	if err != nil || status.Code != OK {
		fmt.Println("GetLog Failure: ", err, status)
	}

	_, status, err = conn.Get([]byte("object000"))
	if err != nil {
		fmt.Println("Get Failure: ", err)
	}

	if status.Code != REMOTE_NOT_AUTHORIZED {
		fmt.Println("SET ACL not effective, ", status)
	}

	// Close the verify connection
	conn.Close()
}
