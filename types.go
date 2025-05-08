package main

import (
	"github.com/decisiveai/event-hub-poc/eventing"
	datacore "github.com/decisiveai/mdai-data-core/variables"
	"go.uber.org/zap"
)

type MdaiInterface struct {
	Logger   *zap.Logger
	Datacore *datacore.ValkeyAdapter
}

type HandlerName string

type HandlerFunc func(MdaiInterface, eventing.MdaiEvent) error

type HandlerMap map[HandlerName]HandlerFunc
