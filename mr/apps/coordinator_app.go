package apps

import (
	"log"
	"os"
	"strconv"

	"github.com/csugulo/6.824/mr/mr"
)

type CoordinatorApp struct{}

func (app *CoordinatorApp) Main(args ...string) {
	if len(args) < 3 {
		log.Fatal("Usage: coordinator pluginPath nReduce fileNames...")
		os.Exit(-1)
	}
	pluginPath := args[0]
	nReduce, _ := strconv.ParseInt(args[1], 10, 64)
	fileNames := args[2:]

	coordinatoor := mr.MakeCoordinator(pluginPath, int(nReduce), fileNames)
	coordinatoor.Server()
}
