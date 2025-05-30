package main

import (
	"context"
	"fmt"
	"time"

	"log"

	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-event-hub/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"

	"os"
	"strings"

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

// ProcessEvent handles an MdaiEvent according to configured workflows
func ProcessEvent(ctx context.Context, client valkey.Client, configMgr ConfigMapManagerInterface, logger *zap.Logger) eventing.HandlerInvoker {
	dataAdapter := datacore.NewHandlerAdapter(client, zapr.NewLogger(logger))

	mdaiInterface := MdaiInterface{
		Datacore: dataAdapter,
		Logger:   logger,
	}
	return func(event eventing.MdaiEvent) error {
		hubName := event.HubName
		if hubName == "" {
			return fmt.Errorf("no hub name provided")
		}
		log.Printf("Processing event for hub: %s", event.HubName)

		workflowMap, err := configMgr.GetConfigMapForHub(ctx, event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap for hub %s: %v", event.HubName, err)
		}

		var workflowFound = false
		logger.Info(fmt.Sprintf("Processing event %s", event.Name))
		// Match on whole name, e.g. "NoisyServiceAlert.firing"
		if workflow, exists := workflowMap[event.Name]; exists {
			workflowFound = true
			for _, automationStep := range workflow {
				err := safePerformAutomationStep(mdaiInterface, automationStep, event)
				if err != nil {
					return err
				}
			}
			// Match on alert name regardless of status, e.g. NoisyServiceAlert
		} else if nameparts := strings.Split(event.Name, "."); len(nameparts) > 1 {
			if workflow, exists := workflowMap[nameparts[0]]; exists {
				workflowFound = true
				for _, automationStep := range workflow {
					err := safePerformAutomationStep(mdaiInterface, automationStep, event)
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

func safePerformAutomationStep(mdai MdaiInterface, autoStep v1.AutomationStep, event eventing.MdaiEvent) error {
	args := autoStep.Arguments
	handlerName := HandlerName(autoStep.HandlerRef)

	if handlerFn, exists := SupportedHandlers[handlerName]; exists {
		// TODO add event audit here
		err := handlerFn(mdai, event, args)
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

func initValKeyClient(ctx context.Context, logger *zap.Logger) (valkey.Client, error) {
	valKeyEndpoint := getEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")
	valkeyPassword := getEnvVariableWithDefault(valkeyPasswordEnvVarKey, "")

	logger.Info(fmt.Sprintf("Initializing valkey client with endpoint %s", valKeyEndpoint))

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
		3*time.Minute,
		5*time.Second,
	)
}

func initEventHub(ctx context.Context, logger *zap.Logger) (eventing.EventHubInterface, error) {
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
		3*time.Minute,
		5*time.Second,
	)
}
