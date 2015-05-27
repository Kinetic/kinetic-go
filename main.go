// The MIT License (MIT)
//
// Copyright (c) 2015 Seagate Technology
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// @author = ["Ignacio Corderi"]

package main

import "github.com/seagate/kinetic-go/kinetic"
import "strconv"
import "fmt"

func regular() {
	client, _ := kinetic.Connect("localhost:8123")
	defer client.Close()
	
	count := 10
	
	rxs := make([]<-chan error, count)
	
	for i := 0; i < count; i++ {
		rxs[i], _ = client.Put([]byte("from-go-" + strconv.Itoa(i)), []byte("refactored 2.0!"))
	}
	
	// wait for all
	for i := 0; i < count; i++ {
		err := <-rxs[i]
		if err != nil {
			fmt.Println(err)
		}
	}
}

func repeat(ch chan []byte, count int, data[] byte) {	
	for i := 0; i < count; i++ {
		ch <- data
	}
	ch <- nil
}

func channeled() {
	client, err := kinetic.Connect("localhost:8123")
	if err != nil {
		fmt.Println(err)
		return
	}	
	defer client.Close()
	
	count := 10
	
	data := make([]byte, 64*1024)
	
	rxs := make([]<-chan error, count)
	
	for i := 0; i < count; i++ {
		ch := make(chan []byte)
		go repeat(ch, 16, data)
		rxs[i], err = client.PutFrom([]byte("go-big-" + strconv.Itoa(i)), 1024*1024, ch)
		if err != nil {
			fmt.Println(err)
		}
	}
	
	// wait for all
	for i := 0; i < count; i++ {
		err := <-rxs[i]
		if err != nil {
			fmt.Println(err)
		}
	}
}

func main() {
	channeled()
}