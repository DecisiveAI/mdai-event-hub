package main

import (
	"encoding/json"
	"fmt"
	"github.com/decisiveai/event-hub-poc/eventing"
	"go.uber.org/zap"
)

const (
	HandleAddNoisyServiceToSet      HandlerName = "HandleAddNoisyServiceToSet"
	HandleRemoveNoisyServiceFromSet HandlerName = "HandleRemoveNoisyServiceFromSet"
	HandleNoisyServiceAlert         HandlerName = "HandleNoisyServiceAlert"
)

// SupportedHandlers Go doesn't support dynamic accessing of exports. So this is a workaround.
// The handler library will have to export a map that can by dynamically accessed.
// To enforce this, handlers are declared with a lower case first character so they
// are not exported directly but can only be accessed through the map
var SupportedHandlers = HandlerMap{
	HandleAddNoisyServiceToSet:      handleAddNoisyServiceToSet,
	HandleRemoveNoisyServiceFromSet: handleRemoveNoisyServiceFromSet,
	HandleNoisyServiceAlert:         handleNoisyServiceList,
}

func processEventPayload(event eventing.MdaiEvent) (map[string]interface{}, error) {
	var payloadData map[string]interface{}

	err := json.Unmarshal([]byte(event.Payload), &payloadData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return payloadData, nil
}

func handleNoisyServiceList(mdai MdaiInterface, event eventing.MdaiEvent) error {
	payloadData, err := processEventPayload(event)
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}
	serviceName := payloadData["service_name"].(string)
	status := payloadData["status"].(string)

	if status == "firing" {
		mdai.Datacore.AddElementToSet("service_list", serviceName)
	} else if status == "resolved" {
		mdai.Datacore.RemoveElementFromSet("service_list", serviceName)
	} else {
		return fmt.Errorf("unknown alert status: %w", status)
	}
	return nil
}

func handleAddNoisyServiceToSet(mdai MdaiInterface, event eventing.MdaiEvent) error {
	payloadData, err := processEventPayload(event)
	mdai.Logger.Info("payload data", zap.Any("payload", payloadData))
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}
	serviceName := payloadData["service_name"].(string)

	result := mdai.Datacore.AddElementToSet("service_list", serviceName)
	mdai.Logger.Info("setter result ", zap.Any("result", result))
	return nil
}

func handleRemoveNoisyServiceFromSet(mdai MdaiInterface, event eventing.MdaiEvent) error {
	payloadData, err := processEventPayload(event)
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}
	serviceName := payloadData["service_name"].(string)

	mdai.Datacore.RemoveElementFromSet("service_list", serviceName)
	return nil
}
