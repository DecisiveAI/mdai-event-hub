package main

import (
	"fmt"
	"github.com/decisiveai/event-hub-poc/eventing"
	datacore "github.com/decisiveai/mdai-data-core/variables"
	"github.com/go-logr/logr"
	"github.com/valkey-io/valkey-go"
	"log"
	"os"
	"strings"
)

const (
	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"
)

// WorkflowMap defines the mapping from event names to handler names
type WorkflowMap map[string][]HandlerName

// GetCurrentWorkflowMap fetches the current workflow configuration
// This function can be implemented to fetch configuration from wherever it's stored
func GetCurrentWorkflowMap() WorkflowMap {
	// In a real implementation, this could fetch from Kubernetes ConfigMap, etc.
	return WorkflowMap{
		"NoisyServiceFired":    {"HandleAddNoisyServiceToSet"},
		"NoisyServiceResolved": {"HandleRemoveNoisyServiceFromSet"},
	}
}

// ProcessEvent handles an MdaiEvent according to configured workflows
func ProcessEvent(client valkey.Client, logger logr.Logger) eventing.HandlerInvoker {
	dataAdapter := datacore.NewValkeyAdapter(client, logger)

	return func(event eventing.MdaiEvent) error {
		// Get the current workflow configuration
		workflowMap := GetCurrentWorkflowMap()

		var workflowFound bool = false

		// Match on whole name, e.g. "NoisyServiceAlert.firing"
		if workflow, exists := workflowMap[event.Name]; exists {
			workflowFound = true
			for _, handlerName := range workflow {
				err := safeInvokeHandler(dataAdapter, handlerName, event)
				if err != nil {
					return err
				}
			}
			// Match on alert name regardless of status, e.g. NoisyServiceAlert
		}
		if nameparts := strings.Split(event.Name, "."); len(nameparts) > 0 {
			if workflow, exists := workflowMap[nameparts[0]]; exists {
				workflowFound = true
				for _, handlerName := range workflow {
					err := safeInvokeHandler(dataAdapter, handlerName, event)
					if err != nil {
						return err
					}
				}
			}
		}

		if !workflowFound {
			return fmt.Errorf("No configured automation for event", "name", event.Name)
		}
		return nil
	}
}

func safeInvokeHandler(adapter *datacore.ValkeyAdapter, handlerName HandlerName, event eventing.MdaiEvent) error {
	if handlerFn, exists := SupportedHandlers[handlerName]; exists {
		handlerFn(adapter, event)
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
	// Set up logger
	logger := logr.New(logr.NewLogger()) // Or your preferred logger

	// Set up valkey client
	client, _ := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{getEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")},
		Password:    getEnvVariableWithDefault(valkeyPasswordEnvVarKey, ""),
	})

	// Create event hub
	hub, err := eventing.NewEventHub("amqp://guest:guest@localhost:5672/", "mdai-events")
	if err != nil {
		log.Fatalf("Failed to create EventHub: %s", err)
	}
	defer hub.Close()

	// Start listening for events
	err = hub.StartListening(logger, ProcessEvent(client, logger))
	if err != nil {
		log.Fatalf("Failed to start listening: %s", err)
	}
}
