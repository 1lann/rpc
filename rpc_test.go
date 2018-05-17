package rpc

import (
	"encoding/gob"
	"net"
	"testing"
	"time"
)

func TestFire(t *testing.T) {
	pa, pb := net.Pipe()
	var a, b *Client

	done := make(chan struct{})
	go func() {
		var err error
		a, err = NewClient(pa)
		if err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	var err error
	b, err = NewClient(pb)
	if err != nil {
		t.Fatal(err)
	}

	<-done

	success := make(chan bool, 2)
	b.On("test", func(payload interface{}) interface{} {
		if str, ok := payload.(string); ok {
			if str == "Hello, world!" {
				success <- true
			} else {
				t.Fatal("payload is not hello world")
				success <- false
			}
		} else {
			t.Fatal("payload is not a string")
			success <- false
		}

		return nil
	})

	go b.Receive()

	a.Fire("test", "Hello, world!")

	go func() {
		<-time.After(time.Second)
		success <- false
	}()

	if !<-success {
		t.Fatal("test unsuccessful")
	}
}

type dataStruct struct {
	Name string
	Age  int
}

func TestDoResponse(t *testing.T) {
	gob.Register(dataStruct{})

	pa, pb := net.Pipe()
	var a, b *Client

	done := make(chan struct{})
	go func() {
		var err error
		a, err = NewClient(pa)
		if err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	var err error
	b, err = NewClient(pb)
	if err != nil {
		t.Fatal(err)
	}

	<-done

	b.On("special", func(payload interface{}) interface{} {
		if p, ok := payload.(dataStruct); ok {
			if p.Name != "Initial" || p.Age != 10 {
				t.Fatal("payload does not match expected value")
			}
		} else {
			t.Fatal("payload is not a dataStruct")
		}

		return dataStruct{
			Name: "Responder",
			Age:  1,
		}
	})

	go b.Receive()
	go a.Receive()

	result, err := a.Do("special", dataStruct{
		Name: "Initial",
		Age:  10,
	})
	if err != nil {
		t.Fatal(err)
	}

	if res, ok := result.(dataStruct); ok {
		if res.Name != "Responder" || res.Age != 1 {
			t.Fatal("payload does not match expected value")
		}
	} else {
		t.Fatal("payload is not a dataStruct")
	}
}
