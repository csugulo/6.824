package apps

import "github.com/csugulo/mr/mr"

type WorkerApp struct{}

func (app *WorkerApp) Main(args ...string) {
	worker := mr.MakeWorker()
	worker.Run()
}
