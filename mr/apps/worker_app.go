package apps

import "github.com/csugulo/6.824/mr/mr"

type WorkerApp struct{}

func (app *WorkerApp) Main(args ...string) {
	worker := mr.MakeWorker()
	worker.Run()
}
