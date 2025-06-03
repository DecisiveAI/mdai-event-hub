package main

import (
	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-event-hub/eventing"
	"go.uber.org/zap"
)

type MdaiInterface struct {
	Logger   *zap.Logger
	Datacore *datacore.HandlerAdapter
}

type HandlerName string

type HandlerFunc func(MdaiInterface, eventing.MdaiEvent, map[string]string) error

type HandlerMap map[HandlerName]HandlerFunc
