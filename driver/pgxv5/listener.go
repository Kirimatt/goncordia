package pgxv5

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goncordia/goncordia/driver"
)

// listener implements driver.Listener using PostgreSQL LISTEN/NOTIFY.
// Each Listen call acquires a dedicated connection from the pool.
type listener struct {
	pool *pgxpool.Pool
}

type subscription struct {
	conn   *pgxpool.Conn
	ch     chan driver.Notification
	queue  string
	cancel context.CancelFunc
}

func (l *listener) Listen(ctx context.Context, queue string) (<-chan driver.Notification, error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for LISTEN: %w", err)
	}

	channel := "goncordia:" + queue
	if _, err := conn.Exec(ctx, "LISTEN "+pgQuoteIdentifier(channel)); err != nil {
		conn.Release()
		return nil, fmt.Errorf("LISTEN %s: %w", channel, err)
	}

	ch := make(chan driver.Notification, 32)
	subCtx, cancel := context.WithCancel(ctx)

	sub := &subscription{conn: conn, ch: ch, queue: queue, cancel: cancel}
	go sub.receiveLoop(subCtx)

	return ch, nil
}

func (s *subscription) receiveLoop(ctx context.Context) {
	defer s.conn.Release()
	defer close(s.ch)

	for {
		n, err := s.conn.Conn().WaitForNotification(ctx)
		if err != nil {
			// ctx cancelled or connection closed — normal shutdown
			return
		}
		_ = n // payload is the job ID; we only need the queue name
		select {
		case s.ch <- driver.Notification{Queue: s.queue}:
		default:
			// channel full — notification dropped; fallback ticker will cover it
		}
	}
}

func (l *listener) Unlisten(ctx context.Context, queue string) error {
	// Connections are released when their context is cancelled via Listen's subCtx.
	// This method is provided for completeness; callers should cancel the Listen context.
	return nil
}

func (l *listener) Close() error { return nil }

// pgQuoteIdentifier wraps an identifier in double quotes and escapes internal quotes.
func pgQuoteIdentifier(s string) string {
	// Simple safe escaping: double any existing double-quotes
	escaped := ""
	for _, c := range s {
		if c == '"' {
			escaped += `""`
		} else {
			escaped += string(c)
		}
	}
	return `"` + escaped + `"`
}
