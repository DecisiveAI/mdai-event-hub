package main

import (
	"context"
	"fmt"
	"github.com/decisiveai/event-hub-poc/eventing"
	datacore "github.com/decisiveai/mdai-data-core/variables"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
)

var (
	logger *zap.Logger
)

const (
	rabbitmqEndpointEnvVarKey = "RABBITMQ_ENDPOINT"
	rabbitmqPasswordEnvVarKey = "RABBITMQ_PASSWORD"

	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"

	//hubNameEnvVarKey = "HUB_NAME"
)

func init() {
	// Define custom encoder configuration
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"                   // Rename the time field
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // Use human-readable timestamps
	encoderConfig.CallerKey = "caller"                    // Show caller file and line number
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // JSON logging with readable timestamps
		zapcore.Lock(os.Stdout),               // Output to stdout
		zap.DebugLevel,                        // Log info and above
	)

	logger = zap.New(core, zap.AddCaller())
	// don't really care about failing of defer that is the last thing run before the program exists
	//nolint:all
	defer logger.Sync() // Flush logs before exiting
}

// WorkflowMap defines the mapping from event names to handler names
type WorkflowMap map[string][]HandlerName

// GetCurrentWorkflowMap fetches the current workflow configuration
// This function can be implemented to fetch configuration from wherever it's stored
func GetCurrentWorkflowMap() WorkflowMap {
	// In a real implementation, this could fetch from Kubernetes ConfigMap, etc.
	return WorkflowMap{
		"NoisyServiceFired":    {HandleAddNoisyServiceToSet},
		"NoisyServiceResolved": {HandleRemoveNoisyServiceFromSet},
	}
}

// ProcessEvent handles an MdaiEvent according to configured workflows
func ProcessEvent(client valkey.Client, logger *zap.Logger) eventing.HandlerInvoker {
	//hubName := getEnvVariableWithDefault(hubNameEnvVarKey, "mdaihub-sample")
	dataAdapter := datacore.NewValkeyAdapter(client, logger, "mdaihub-sample")

	mdaiInterface := MdaiInterface{
		Datacore: dataAdapter,
		Logger:   logger,
	}
	dataAdapter.Logger.Info("DataAdapter initialized")
	return func(event eventing.MdaiEvent) error {
		// Get the current workflow configuration
		workflowMap := GetCurrentWorkflowMap()

		var workflowFound bool = false
		logger.Info(fmt.Sprintf("Processing event %s", event.Name))
		// Match on whole name, e.g. "NoisyServiceAlert.firing"
		if workflow, exists := workflowMap[event.Name]; exists {
			workflowFound = true
			for _, handlerName := range workflow {
				err := safeInvokeHandler(mdaiInterface, handlerName, event)
				if err != nil {
					return err
				}
			}
			// Match on alert name regardless of status, e.g. NoisyServiceAlert
		} else if nameparts := strings.Split(event.Name, "."); len(nameparts) > 0 {
			if workflow, exists := workflowMap[nameparts[0]]; exists {
				workflowFound = true
				for _, handlerName := range workflow {
					err := safeInvokeHandler(mdaiInterface, handlerName, event)
					if err != nil {
						return err
					}
				}
			}
		}

		if !workflowFound {
			logger.Info("No configured automation for event", zap.String("name", event.Name))
			return nil // Don't treat this as an error, just log a warning
		}
		return nil
	}
}

func safeInvokeHandler(mdai MdaiInterface, handlerName HandlerName, event eventing.MdaiEvent) error {
	if handlerFn, exists := SupportedHandlers[handlerName]; exists {
		err := handlerFn(mdai, event)
		if err != nil {
			return fmt.Errorf("handler %s failed: %w", handlerName, err)
		}
		return nil
	}
	return fmt.Errorf("handler %s not supported", handlerName)
}

func getEnvVariableWithDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func main() {

	var (
		valkeyClient valkey.Client
		retryCount   int
	)

	ctx := context.Background()

	operation := func() (string, error) {
		var err error
		valKeyEndpoint := getEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")
		valkeyPassword := getEnvVariableWithDefault(valkeyPasswordEnvVarKey, "")

		valkeyClient, err = valkey.NewClient(valkey.ClientOption{
			InitAddress: []string{valKeyEndpoint},
			Password:    valkeyPassword,
		})
		if err != nil {
			retryCount++
			return "", err
		}
		logger.Info(fmt.Sprintf("Initializing valkey client with endpoint %s", valKeyEndpoint))
		return "", nil
	}
	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.InitialInterval = 5 * time.Second

	notifyFunc := func(err error, duration time.Duration) {
		logger.Error("failed to initialize valkey client. retrying...", zap.Int("retry_count", retryCount), zap.Duration("duration", duration))
	}

	if _, err := backoff.Retry(ctx, operation,
		backoff.WithBackOff(backoff.NewExponentialBackOff()),
		backoff.WithMaxElapsedTime(3*time.Minute),
		backoff.WithNotify(notifyFunc)); err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}

	// Create event hub
	rmqEndpoint := getEnvVariableWithDefault(rabbitmqEndpointEnvVarKey, "")
	rmqPassword := getEnvVariableWithDefault(rabbitmqPasswordEnvVarKey, "")

	// Log connection parameters (but mask the password)
	logger.Info("Connecting to RabbitMQ",
		zap.String("endpoint", rmqEndpoint),
		zap.String("queue", "mdai-events"))

	hub, err := eventing.NewEventHub("amqp://mdai:"+rmqPassword+"@"+rmqEndpoint+"/", "mdai-events", logger)
	if err != nil {
		logger.Fatal("Failed to create EventHub", zap.Error(err))
	}
	defer hub.Close()

	// Start listening and block until termination signal
	// This handles all the signal processing internally
	err = hub.ListenUntilSignal(ProcessEvent(valkeyClient, logger))
	if err != nil {
		logger.Fatal("Failed to start event listener", zap.Error(err))
	}

	logger.Info("Service shutting down")
}
