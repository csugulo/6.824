package main

import (
	"log"
	"os"
	"strings"

	apps "github.com/csugulo/6.824/mr/apps"
)

const (
	MR_COORDINATOR_MAIN = "coordinator"
	MR_WORKER_MAIN      = "worker"
	MR_SEQUENTIAL       = "mrsequential"
)

func main() {
	splited := strings.Split(os.Args[0], "/")
	appName := splited[0]
	if len(splited) > 1 {
		appName = splited[len(splited)-1]
	}
	switch appName {
	case MR_COORDINATOR_MAIN:
		app := &apps.CoordinatorApp{}
		app.Main(os.Args[1:]...)
	case MR_WORKER_MAIN:
		app := &apps.WorkerApp{}
		app.Main(os.Args[1:]...)
	case MR_SEQUENTIAL:
		app := &apps.MrSequential{}
		app.Main(os.Args)
	default:
		log.Fatalf("Unknown app name: %v", os.Args[0])
		os.Exit(1)
	}
}
