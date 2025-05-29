package eventing

type EventHubInterface interface {
	PublishMessage(event MdaiEvent) error
	StartListening(invoker HandlerInvoker) error
	ListenUntilSignal(invoker HandlerInvoker) error
	Close()
}

var _ EventHubInterface = (*EventHub)(nil)
