package main

import (
	"context"

	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-event-hub/eventing"
	"go.uber.org/zap"
)

type MdaiComponents struct {
	Logger   *zap.Logger
	Datacore *datacore.HandlerAdapter
}

type HandlerName string

type HandlerFunc func(context.Context, MdaiComponents, eventing.MdaiEvent, map[string]string) error

type HandlerMap map[HandlerName]HandlerFunc
