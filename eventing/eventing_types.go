package eventing

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

// HandlerInvoker is a function type that processes MdaiEvents
type HandlerInvoker func(event MdaiEvent) error

// EventHub represents a connection to RabbitMQ
type EventHub struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueName   string
	mu          sync.Mutex
	isListening bool
}

type MdaiEvent struct {
	Name      string `json:"name"`
	Source    string `json:"source"`
	Id        string `json:"id,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Payload   string `json:"payload,omitempty"`
}
