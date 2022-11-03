package beanstalk

import (
	"context"
	"errors"
	"sync"

	"go.opencensus.io/trace"
)

// Producer maintains a pool of connections to beanstalk servers on which it
// inserts jobs.
type Producer struct {
	cancel    func()
	config    Config
	producers []*producer
	mu        sync.RWMutex

	producersCh chan *producer
}

// NewProducer returns a new Producer.
func NewProducer(uris []string, config Config) (*Producer, error) {
	if !config.IgnoreURIValidation {
		if err := ValidURIs(uris); err != nil {
			return nil, err
		}
	}

	// Create a context that can be cancelled to stop the producers.
	ctx, cancel := context.WithCancel(context.Background())

	// Create the pool and spin up the producers.
	pool := &Producer{cancel: cancel, config: config.normalize()}

	// Initialize producer buffered channel
	pool.producersCh = make(chan *producer, len(uris)*pool.config.Multiply)

	for _, uri := range multiply(uris, pool.config.Multiply) {
		p := &producer{errC: make(chan error, 1)}
		go maintainConn(ctx, uri, pool.config, connHandler{
			handle: p.setConnection,
		})

		pool.producers = append(pool.producers, p)
		pool.producersCh <- p
	}

	return pool, nil
}

// Stop this producer.
func (pool *Producer) Stop() {
	pool.cancel()

	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.producers = []*producer{}

	producersCh := pool.producersCh
	if producersCh != nil {
		pool.producersCh = nil
		close(producersCh)
	}
	// TODO: close producer connections?
}

// IsConnected returns true when at least one producer in the pool is connected.
func (pool *Producer) IsConnected() bool {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	for _, p := range pool.producers {
		if p.isConnected() {
			return true
		}
	}

	return false
}

func (pool *Producer) getProducer(ctx context.Context) (*producer, error) {
	pool.mu.RLock()
	producersCh := pool.producersCh
	pool.mu.RUnlock()

	if producersCh == nil {
		return nil, ErrDisconnected
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p, done := <-producersCh:
		if !done {
			return nil, ErrDisconnected
		}
		return p, nil
	}
}

// Put a job into the specified tube.
func (pool *Producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Producer.Put")
	defer span.End()

	// Note that this loop is very experimental,
	// If all connections are bad, it will occupy all producers available until the work is done.
	for i := 0; i < cap(pool.producersCh); i++ {
		p, err := pool.getProducer(ctx)
		if err != nil {
			return 0, err
		}
		defer func() {
			pool.mu.RLock()
			defer pool.mu.RUnlock()

			if pool.producersCh != nil {
				pool.producersCh <- p
			}
		}()

		id, err := p.Put(ctx, tube, body, params)
		// If the job is too big, assume it'll be too big for the other
		// beanstalk producers as well and return the error.
		if errors.Is(err, ErrTooBig) {
			return 0, err
		}
		if err != nil {
			continue
		}

		return id, nil
	}

	return 0, ErrDisconnected
}

type producer struct {
	conn *Conn
	errC chan error
	mu   sync.RWMutex
}

func (producer *producer) setConnection(ctx context.Context, conn *Conn) error {
	producer.mu.Lock()
	producer.conn = conn
	producer.mu.Unlock()

	select {
	// If an error occurred in Put, return it.
	case err := <-producer.errC:
		return err

	// Exit when this producer is closing down.
	case <-ctx.Done():
		producer.mu.Lock()
		producer.conn = nil
		producer.mu.Unlock()

		return nil
	}
}

// isConnected returns true if this producer is connected.
func (producer *producer) isConnected() bool {
	producer.mu.RLock()
	defer producer.mu.RUnlock()

	return producer.conn != nil
}

// Put inserts a job into beanstalk.
func (producer *producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/producer.Put")
	defer span.End()

	producer.mu.Lock()
	defer producer.mu.Unlock()

	// If this producer isn't connected, return ErrDisconnected.
	if producer.conn == nil {
		return 0, ErrDisconnected
	}

	// Insert the job. If this fails, mark the connection as disconnected and
	// report the error back to setConnection.
	id, err := producer.conn.Put(ctx, tube, body, params)
	switch {
	// ErrTooBig is a recoverable error.
	case errors.Is(err, ErrTooBig):
	case err != nil:
		producer.conn = nil
		producer.errC <- err
	}

	return id, err
}
