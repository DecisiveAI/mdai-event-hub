package main

import (
	"context"
	"testing"

	"github.com/decisiveai/event-hub-poc/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	valkeyMock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// Mock handler for testing
var testHandlerCalled bool
var testHandlerError error

func testHandler(mdai MdaiInterface, event eventing.MdaiEvent, args map[string]string) error {
	testHandlerCalled = true
	return testHandlerError
}

func TestProcessEvent_Success(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()
	mockConfigMgr := NewMockConfigMapManager()

	testHandlerCalled = false
	testHandlerError = nil

	originalHandlers := SupportedHandlers
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}
	defer func() { SupportedHandlers = originalHandlers }()

	workflowMap := map[string][]v1.AutomationStep{
		"TestAlert.firing": {
			{
				HandlerRef: "testHandler",
				Arguments:  map[string]string{"key": "value"},
			},
		},
	}

	mockConfigMgr.SetConfig("test-hub", workflowMap)

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	handler := ProcessEvent(ctx, mockClient, mockConfigMgr, logger)
	err := handler(event)

	assert.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestProcessEvent_NoHubName(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()
	mockConfigMgr := NewMockConfigMapManager()

	event := eventing.MdaiEvent{
		Name: "TestAlert.firing",
	}

	handler := ProcessEvent(ctx, mockClient, mockConfigMgr, logger)
	err := handler(event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no hub name provided")
}

func TestProcessEvent_MatchAlertNameOnly(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()
	mockConfigMgr := NewMockConfigMapManager()

	testHandlerCalled = false
	testHandlerError = nil

	originalHandlers := SupportedHandlers
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}
	defer func() { SupportedHandlers = originalHandlers }()

	workflowMap := map[string][]v1.AutomationStep{
		"TestAlert": {
			{
				HandlerRef: "testHandler",
				Arguments:  map[string]string{"key": "value"},
			},
		},
	}
	mockConfigMgr.SetConfig("test-hub", workflowMap)

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	handler := ProcessEvent(ctx, mockClient, mockConfigMgr, logger)
	err := handler(event)

	assert.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestProcessEvent_NoWorkflowFound(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()
	mockConfigMgr := NewMockConfigMapManager()

	mockConfigMgr.SetConfig("test-hub", map[string][]v1.AutomationStep{})

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "UnknownAlert.firing",
	}

	handler := ProcessEvent(ctx, mockClient, mockConfigMgr, logger)
	err := handler(event)

	assert.NoError(t, err)
}

func TestSafePerformAutomationStep_Success(t *testing.T) {
	testHandlerCalled = false
	testHandlerError = nil

	originalHandlers := SupportedHandlers
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}
	defer func() { SupportedHandlers = originalHandlers }()

	mdai := MdaiInterface{
		Logger: zap.NewNop(),
	}

	autoStep := v1.AutomationStep{
		HandlerRef: "testHandler",
		Arguments:  map[string]string{"key": "value"},
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	err := safePerformAutomationStep(mdai, autoStep, event)

	assert.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestSafePerformAutomationStep_UnsupportedHandler(t *testing.T) {
	mdai := MdaiInterface{
		Logger: zap.NewNop(),
	}

	autoStep := v1.AutomationStep{
		HandlerRef: "unsupportedHandler",
		Arguments:  map[string]string{"key": "value"},
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	err := safePerformAutomationStep(mdai, autoStep, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler unsupportedHandler not supported")
}
