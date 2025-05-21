package main

import (
	"github.com/decisiveai/event-hub-poc/eventing"
	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"go.uber.org/zap"
)

type MdaiInterface struct {
	Logger   *zap.Logger
	Datacore *datacore.HandlerAdapter
}

type HandlerName string

type HandlerFunc func(MdaiInterface, eventing.MdaiEvent, map[string]string) error

type HandlerMap map[HandlerName]HandlerFunc
