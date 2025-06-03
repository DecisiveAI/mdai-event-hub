package eventing

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

func (h *EventHub) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("queue_name", h.queueName)
	enc.AddBool("is_listening", h.isListening)
	return nil
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

func (mdaiEvent MdaiEvent) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", mdaiEvent.Name)
	enc.AddString("id", mdaiEvent.Id)
	enc.AddString("source", mdaiEvent.Source)
	enc.AddString("hub_name", mdaiEvent.HubName)
	enc.AddString("payload", mdaiEvent.Payload)
	enc.AddTime("timestamp", mdaiEvent.Timestamp)
	enc.AddString("correlation_id", mdaiEvent.CorrelationId)
	return nil
}

func (mdaiEvent *MdaiEvent) ApplyDefaults() {
	if mdaiEvent.Id == "" {
		mdaiEvent.Id = createEventUuid()
	}
	if mdaiEvent.Timestamp.IsZero() {
		mdaiEvent.Timestamp = time.Now()
	}
}

func (mdaiEvent MdaiEvent) Validate() error {
	if mdaiEvent.Name == "" {
		return errors.New("missing required field: name")
	}

	if mdaiEvent.HubName == "" {
		return errors.New("missing required field: name")
	}

	if mdaiEvent.Payload == "" {
		return errors.New("missing required field: name")
	}
	return nil
}

func createEventUuid() string {
	id := uuid.New()
	return id.String()
}
