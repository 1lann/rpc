package rpc

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

const handshake = "1lann/rpc: handshake"

// Packet represents a packet that is serialized for lidar use.
type Packet struct {
	Event       string
	ID          string
	ExpectReply bool
	Payload     interface{}
}

// Handler represents the handler used for receiving events.
type Handler func(payload interface{}) interface{}

// Client represents a client.
type Client struct {
	conn    net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
	writer  *gzip.Writer

	sendMutex    *sync.Mutex
	receiptMutex *sync.Mutex

	handlers map[string]Handler
	receipts map[string]chan<- interface{}
}

func init() {
	gob.Register(Packet{})
}

// NewClient returns a new client on the given connection.
func NewClient(conn net.Conn) (*Client, error) {
	if tcpconn, ok := conn.(*net.TCPConn); ok {
		tcpconn.SetKeepAlive(true)
		tcpconn.SetKeepAlivePeriod(5 * time.Second)
	}

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	wr := gzip.NewWriter(conn)

	done := make(chan struct{})
	go func() {
		wr.Write([]byte(handshake))
		wr.Flush()
		close(done)
	}()

	rd, err := gzip.NewReader(conn)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, len(handshake))
	if _, err := io.ReadFull(rd, buffer); err != nil {
		return nil, err
	}

	if !bytes.Equal(buffer, []byte(handshake)) {
		return nil, errors.New("rpc: invalid handshake")
	}

	<-done

	conn.SetDeadline(time.Time{})

	return &Client{
		conn:    conn,
		encoder: gob.NewEncoder(wr),
		decoder: gob.NewDecoder(rd),
		writer:  wr,

		sendMutex:    new(sync.Mutex),
		receiptMutex: new(sync.Mutex),

		handlers: make(map[string]Handler),
		receipts: make(map[string]chan<- interface{}),
	}, nil
}

// On registers a handler for handling an event.
func (c *Client) On(event string, handler Handler) {
	c.handlers[event] = handler
}

// Receive runs the receive loop to process incoming packets. It returns
// when the connection dies, and a new client will need to be spawned.
func (c *Client) Receive() error {
	for {
		var p Packet
		if err := c.decoder.Decode(&p); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}

			return err
		}

		c.receiptMutex.Lock()
		receipt, found := c.receipts[p.ID]
		c.receiptMutex.Unlock()
		if found {
			receipt <- p.Payload
			continue
		}

		if p.Event == "__reply__" {
			continue
		}

		if handler, found := c.handlers[p.Event]; found {
			resp := handler(p.Payload)
			if p.ExpectReply {
				go c.fire("__reply__", p.ID, false, resp)
			}
		} else {
			log.Println("rpc: unknown handler for event:", p.Event)
			continue
		}
	}
}

func (c *Client) fire(event, id string, expectReply bool, payload interface{}) error {
	c.sendMutex.Lock()
	defer c.sendMutex.Unlock()

	if err := c.encoder.Encode(Packet{
		Event:       event,
		ID:          id,
		ExpectReply: expectReply,
		Payload:     payload,
	}); err != nil {
		return err
	}

	return c.writer.Flush()
}

// Fire fires an event with the given payload asynchronously.
func (c *Client) Fire(event string, payload interface{}) error {
	return c.fire(event, uuid.New().String(), false, payload)
}

// Do performs a blocking request that blocks until the handler on the
// remote end responds. The value of the response is returned. Errors
// from the remote end are moved into the second return argument additionally.
func (c *Client) Do(event string, payload interface{}) (interface{}, error) {
	id := uuid.New().String()
	receive := make(chan interface{}, 1)

	c.receiptMutex.Lock()
	c.receipts[id] = receive
	c.receiptMutex.Unlock()

	defer func() {
		c.receiptMutex.Lock()
		delete(c.receipts, id)
		c.receiptMutex.Unlock()
	}()

	if err := c.fire(event, id, true, payload); err != nil {
		return nil, err
	}

	result := <-receive
	if err, ok := result.(error); ok {
		return nil, err
	}

	return result, nil
}

// Close closes the underlying client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
