package eventing

type MockEventHub struct {
	PublishMessageFunc    func(event MdaiEvent) error
	StartListeningFunc    func(invoker HandlerInvoker) error
	ListenUntilSignalFunc func(invoker HandlerInvoker) error
	CloseFunc             func()

	// Track calls for verification
	PublishedEvents         []MdaiEvent
	ListeningStarted        bool
	ListenUntilSignalCalled bool
	Closed                  bool
}

func NewMockEventHub() *MockEventHub {
	return &MockEventHub{
		PublishMessageFunc:    func(event MdaiEvent) error { return nil },
		StartListeningFunc:    func(invoker HandlerInvoker) error { return nil },
		ListenUntilSignalFunc: func(invoker HandlerInvoker) error { return nil },
		CloseFunc:             func() {},
		PublishedEvents:       make([]MdaiEvent, 0),
	}
}

func (m *MockEventHub) PublishMessage(event MdaiEvent) error {
	m.PublishedEvents = append(m.PublishedEvents, event)
	return m.PublishMessageFunc(event)
}

func (m *MockEventHub) StartListening(invoker HandlerInvoker) error {
	m.ListeningStarted = true
	return m.StartListeningFunc(invoker)
}

func (m *MockEventHub) ListenUntilSignal(invoker HandlerInvoker) error {
	m.ListenUntilSignalCalled = true
	return m.ListenUntilSignalFunc(invoker)
}

func (m *MockEventHub) Close() {
	m.Closed = true
	m.CloseFunc()
}

// Helper methods for assertions
func (m *MockEventHub) GetPublishedEvents() []MdaiEvent {
	return m.PublishedEvents
}

func (m *MockEventHub) WasListeningStarted() bool {
	return m.ListeningStarted
}

func (m *MockEventHub) WasListenUntilSignalCalled() bool {
	return m.ListenUntilSignalCalled
}

func (m *MockEventHub) WasClosed() bool {
	return m.Closed
}

func (m *MockEventHub) Reset() {
	m.PublishedEvents = make([]MdaiEvent, 0)
	m.ListeningStarted = false
	m.ListenUntilSignalCalled = false
	m.Closed = false
}
