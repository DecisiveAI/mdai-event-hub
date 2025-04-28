package eventing

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// NewEventHub creates a new connection to RabbitMQ
func NewEventHub(connectionString string, queueName string) (*EventHub, error) {
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &EventHub{
		conn:      conn,
		ch:        ch,
		queueName: q.Name,
	}, nil
}

// Close closes the connection to RabbitMQ
func (h *EventHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ch != nil {
		h.ch.Close()
	}
	if h.conn != nil {
		h.conn.Close()
	}
}

// PublishMessage publishes an MdaiEvent to the queue
func (h *EventHub) PublishMessage(event MdaiEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

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
			ContentType: "application/json",
			Body:        jsonData,
		})

	if err != nil {
		return err
	}

	log.Printf("Published event: %s (ID: %s)", event.Name, event.Id)
	return nil
}

// StartListening starts listening for messages and processes them using the provided handler
func (h *EventHub) StartListening(logger *zap.Logger, invoker HandlerInvoker) error {
	h.mu.Lock()
	if h.isListening {
		h.mu.Unlock()
		return nil // Already listening
	}
	h.isListening = true
	h.mu.Unlock()

	msgs, err := h.ch.Consume(
		h.queueName, // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		h.mu.Lock()
		h.isListening = false
		h.mu.Unlock()
		return err
	}

	go func() {
		for d := range msgs {
			logger.Info("Received message", zap.Int("size", len(d.Body)))

			// Parse the message as MdaiEvent
			var event MdaiEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				logger.Error("Failed to parse event", zap.Error(err), zap.String("body", string(d.Body)))
				continue
			}

			// Process the event with the provided handler
			if err := invoker(event); err != nil {
				logger.Error("Failed to invoke event", zap.Error(err), zap.String("body", string(d.Body)))
			}
		}
	}()

	logger.Info("Listening for events", zap.String("queue", h.queueName))
	return nil
}
