package kinetic

import (
	"bytes"
	"fmt"
)

func ExampleBlockPutGetDelete() {
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

func ExampleNonBlockPutGetDelete() {
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

func ExampleNonBlockMuliplePut() {
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
