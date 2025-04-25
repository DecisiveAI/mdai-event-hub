package main

import (
	"github.com/decisiveai/event-hub-poc/eventing"
	datacore "github.com/decisiveai/mdai-data-core/variables"
)

type HandlerName string

type HandlerFunc func(*datacore.ValkeyAdapter, eventing.MdaiEvent)

type HandlerMap map[HandlerName]HandlerFunc
