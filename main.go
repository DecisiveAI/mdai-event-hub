package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-event-hub/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"

	"github.com/go-logr/zapr"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger *zap.Logger
)

const (
	rabbitmqEndpointEnvVarKey = "RABBITMQ_ENDPOINT"
	rabbitmqPasswordEnvVarKey = "RABBITMQ_PASSWORD"

	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"

	automationConfigMapNamePostfix = "-automation"

	retryInitializerMaxElapsedTime  = 3 * time.Minute
	retryInitializerInitialInterval = 5 * time.Second
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
	//nolint:errcheck
	defer logger.Sync() // Flush logs before exiting
}

// ProcessEvent handles an MdaiEvent according to configured workflows
func ProcessEvent(ctx context.Context, client valkey.Client, configMgr ConfigMapManagerInterface, logger *zap.Logger) eventing.HandlerInvoker {
	dataAdapter := datacore.NewHandlerAdapter(client, zapr.NewLogger(logger))

	mdaiInterface := MdaiComponents{
		Datacore: dataAdapter,
		Logger:   logger,
	}
	return func(event eventing.MdaiEvent) error {
		hubName := event.HubName
		if hubName == "" {
			return errors.New("no hub name provided")
		}
		logger.Info("Processing event for hub", zap.String("hub_name", hubName))

		workflowMap, err := configMgr.GetConfigMapForHub(ctx, event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap for hub %s: %w", hubName, err)
		}

		logger.Info("Processing event", zap.String("event_name", event.Name))
		var workflow []v1.AutomationStep

		workflow, exists := workflowMap[event.Name]
		if !exists {
			if parts := strings.Split(event.Name, "."); len(parts) > 1 {
				workflow = workflowMap[parts[0]]
			}
		}

		if len(workflow) == 0 {
			logger.Info("No configured automation for event", zap.String("name", event.Name))
			return nil
		}

		for _, automationStep := range workflow {
			if err := safePerformAutomationStep(mdaiInterface, automationStep, event); err != nil {
				return err
			}
		}
		return nil
	}
}

func safePerformAutomationStep(mdai MdaiComponents, autoStep v1.AutomationStep, event eventing.MdaiEvent) error {
	ctx := context.Background()
	args := autoStep.Arguments
	handlerName := HandlerName(autoStep.HandlerRef)

	if handlerFn, exists := SupportedHandlers[handlerName]; exists {
		// TODO add event audit here
		if err := handlerFn(ctx, mdai, event, args); err != nil {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	valkeyClient, err := initValKeyClient(ctx, logger)
	if err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}

	hub, err := initEventHub(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to create EventHub", zap.Error(err))
	}
	defer hub.Close()

	configMgr, err := NewConfigMapManager(automationConfigMapNamePostfix)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	defer configMgr.Cleanup()

	// Start listening and block until termination signal
	err = hub.ListenUntilSignal(ProcessEvent(ctx, valkeyClient, configMgr, logger))
	if err != nil {
		logger.Fatal("Failed to start event listener", zap.Error(err))
	}

	logger.Info("Service shutting down")
}

func initValKeyClient(ctx context.Context, logger *zap.Logger) (valkey.Client, error) { //nolint:ireturn
	valKeyEndpoint := getEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")
	valkeyPassword := getEnvVariableWithDefault(valkeyPasswordEnvVarKey, "")

	logger.Info("Initializing valkey client with endpoint " + valKeyEndpoint)

	initializer := func() (valkey.Client, error) {
		return valkey.NewClient(valkey.ClientOption{
			InitAddress: []string{valKeyEndpoint},
			Password:    valkeyPassword,
		})
	}

	return RetryInitializer(
		ctx,
		logger,
		"valkey client",
		initializer,
		retryInitializerMaxElapsedTime,
		retryInitializerInitialInterval,
	)
}

func initEventHub(ctx context.Context, logger *zap.Logger) (eventing.EventHubInterface, error) { //nolint:ireturn
	rmqEndpoint := getEnvVariableWithDefault(rabbitmqEndpointEnvVarKey, "")
	rmqPassword := getEnvVariableWithDefault(rabbitmqPasswordEnvVarKey, "")

	logger.Info("Connecting to RabbitMQ",
		zap.String("endpoint", rmqEndpoint),
		zap.String("queue", eventing.EventQueueName))

	initializer := func() (eventing.EventHubInterface, error) {
		return eventing.NewEventHub("amqp://mdai:"+rmqPassword+"@"+rmqEndpoint+"/", eventing.EventQueueName, logger)
	}

	return RetryInitializer(
		ctx,
		logger,
		"event hub",
		initializer,
		retryInitializerMaxElapsedTime,
		retryInitializerInitialInterval,
	)
}
