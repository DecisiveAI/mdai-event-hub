package main

import (
	"context"
	"testing"

	"github.com/decisiveai/mdai-event-hub/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeyMock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// Mock handler for testing
var (
	testHandlerCalled bool
	testHandlerError  error //nolint:errname
)

func testHandler(_ context.Context, _ MdaiComponents, _ eventing.MdaiEvent, _ map[string]string) error {
	testHandlerCalled = true
	return testHandlerError
}

func TestProcessEvent_Success(t *testing.T) {
	ctx := t.Context()

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

	require.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestProcessEvent_NoHubName(t *testing.T) {
	ctx := t.Context()

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

	require.Error(t, err)
	assert.EqualError(t, err, "no hub name provided")
}

func TestProcessEvent_MatchAlertNameOnly(t *testing.T) {
	ctx := t.Context()

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

	require.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestProcessEvent_NoWorkflowFound(t *testing.T) {
	ctx := t.Context()

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

	require.NoError(t, err)
}

func TestSafePerformAutomationStep_Success(t *testing.T) {
	testHandlerCalled = false
	testHandlerError = nil

	originalHandlers := SupportedHandlers
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}
	defer func() { SupportedHandlers = originalHandlers }()

	mdai := MdaiComponents{
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

	require.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestSafePerformAutomationStep_UnsupportedHandler(t *testing.T) {
	mdai := MdaiComponents{
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

	require.Error(t, err)
	assert.EqualError(t, err, "handler unsupportedHandler not supported")
}
