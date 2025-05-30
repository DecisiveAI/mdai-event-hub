package eventing

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"sync"
	"time"
)

// HandlerInvoker is a function type that processes MdaiEvents
type HandlerInvoker func(event MdaiEvent) error

// EventHub represents a connection to RabbitMQ
type EventHub struct {
	conn          *amqp.Connection
	ch            *amqp.Channel
	queueName     string
	mu            sync.Mutex
	isListening   bool
	shutdown      chan struct{}
	processingWg  sync.WaitGroup
	logger        *zap.Logger
	connCloseChan chan *amqp.Error
}

// MdaiEvent represents an event in the system
type MdaiEvent struct {
	Id            string    `json:"id,omitempty"`
	Name          string    `json:"name"`
	Timestamp     time.Time `json:"timestamp,omitempty"`
	Payload       string    `json:"payload"`
	Source        string    `json:"source"`
	CorrelationId string    `json:"correlationId,omitempty"`
	HubName       string    `json:"hubName"`
}
