package mr

type Request struct {
	WorkerID  string          `json:"worker_id"`
	Tasks     map[string]Task `json:"tasks"`
	FetchTask bool            `json:"fetch_task"`
}

type Response struct {
	StatusCode    int    `json:"status_code"`
	StatusMessage string `json:"status_message"`
	Task          *Task  `json:"task"`
}
