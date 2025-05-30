package eventing

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EventQueueName = "mdai-events"
)

// NewEventHub creates a new connection to RabbitMQ
func NewEventHub(connectionString string, queueName string, logger *zap.Logger) (*EventHub, error) {
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable - make queue persistent
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &EventHub{
		conn:          conn,
		ch:            ch,
		queueName:     q.Name,
		shutdown:      make(chan struct{}),
		connCloseChan: make(chan *amqp.Error, 1),
		logger:        logger,
	}, nil
}

// Close closes the connection to RabbitMQ and waits for processing to complete
func (h *EventHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ch != nil {
		_ = h.ch.Close()
		h.ch = nil
	}
	if h.conn != nil {
		_ = h.conn.Close()
		h.conn = nil
	}

	// Wait for all in-flight message processing to complete
	h.processingWg.Wait()
	h.logger.Info("All message processing completed")
}

// PublishMessage publishes an MdaiEvent to the queue
func (h *EventHub) PublishMessage(event MdaiEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ch == nil || h.conn == nil {
		return fmt.Errorf("connection is closed")
	}

	// Serialize the event to JSON
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = h.ch.PublishWithContext(ctx,
		"",          // exchange
		h.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         jsonData,
			DeliveryMode: amqp.Persistent, // Make messages persistent
		})

	if err != nil {
		return err
	}

	h.logger.Info("Published event",
		zap.String("event", event.Name),
		zap.String("id", event.Id))
	return nil
}

// StartListening starts listening for messages and processes them using the provided handler
func (h *EventHub) StartListening(invoker HandlerInvoker) error {
	h.mu.Lock()
	if h.isListening {
		h.mu.Unlock()
		return nil // Already listening
	}
	h.isListening = true
	h.mu.Unlock()

	// Setup prefetch - don't overwhelm this consumer
	err := h.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		h.mu.Lock()
		h.isListening = false
		h.mu.Unlock()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Set up the consumer with manual acknowledgments
	msgs, err := h.ch.Consume(
		h.queueName, // queue
		"",          // consumer
		false,       // auto-ack - changed to false for manual acks
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		h.mu.Lock()
		h.isListening = false
		h.mu.Unlock()
		return fmt.Errorf("failed to consume: %w", err)
	}

	// Monitor the connection for closures
	h.connCloseChan = h.conn.NotifyClose(make(chan *amqp.Error, 1))

	// Start consumer in a goroutine
	go func() {
		for {
			select {
			case <-h.shutdown:
				h.logger.Info("Shutting down consumer")
				return

			case err := <-h.connCloseChan:
				if err != nil {
					h.logger.Error("RabbitMQ connection closed", zap.Error(err))
				} else {
					h.logger.Info("RabbitMQ connection closed gracefully")
				}
				return

			case d, ok := <-msgs:
				if !ok {
					h.logger.Warn("Message channel closed")
					return
				}

				h.logger.Info("Received message", zap.Int("size", len(d.Body)))

				// Track this message as being processed
				h.processingWg.Add(1)

				// Process in a separate goroutine to allow for parallel processing
				// while still tracking completion with WaitGroup
				go func(delivery amqp.Delivery) {
					defer h.processingWg.Done()

					// Parse the message as MdaiEvent
					var event MdaiEvent
					if err := json.Unmarshal(delivery.Body, &event); err != nil {
						h.logger.Error("Failed to parse event",
							zap.Error(err),
							zap.String("body", string(delivery.Body)))

						// Acknowledge the message even if we couldn't parse it
						// to avoid redelivery of unparseable messages
						if err := delivery.Ack(false); err != nil {
							h.logger.Error("Failed to acknowledge message", zap.Error(err))
						}
						return
					}

					// Process the event with the provided handler
					err := invoker(event)
					if err != nil {
						h.logger.Error("Failed to process event",
							zap.Error(err),
							zap.String("eventId", event.Id),
							zap.String("eventName", event.Name))

						// Reject the message but don't requeue if it's a permanent error
						// This is a simplified approach - you might want more sophisticated retry logic
						if err := delivery.Reject(false); err != nil {
							h.logger.Error("Failed to reject message", zap.Error(err))
						}
					} else {
						// Acknowledge successful processing
						if err := delivery.Ack(false); err != nil {
							h.logger.Error("Failed to acknowledge message", zap.Error(err))
						}

						h.logger.Info("Successfully processed event",
							zap.String("eventId", event.Id),
							zap.String("eventName", event.Name),
							zap.String("hubName", event.HubName))
					}
				}(d)
			}
		}
	}()

	h.logger.Info("Listening for events", zap.String("queue", h.queueName))
	return nil
}

// ListenUntilSignal starts the event listener and blocks until a termination signal is received
// This method handles graceful shutdown when the process receives SIGINT or SIGTERM
func (h *EventHub) ListenUntilSignal(invoker HandlerInvoker) error {
	// Start listening for events
	err := h.StartListening(invoker)
	if err != nil {
		return err
	}

	// Create a channel to listen for termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until a termination signal is received
	sig := <-sigChan
	h.logger.Info("Received signal, shutting down gracefully", zap.String("signal", sig.String()))

	// Initiate shutdown
	close(h.shutdown)

	// Set a timeout for graceful shutdown
	shutdownTimeout := 30 * time.Second
	shutdownComplete := make(chan struct{})

	go func() {
		// Close connections - this will also wait for processing to complete
		h.Close()
		close(shutdownComplete)
	}()

	// Wait for shutdown to complete or timeout
	select {
	case <-shutdownComplete:
		h.logger.Info("Graceful shutdown completed")
	case <-time.After(shutdownTimeout):
		h.logger.Warn("Graceful shutdown timed out, forcing exit", zap.Duration("timeout", shutdownTimeout))
	}

	return nil
}
