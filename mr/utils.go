package mr

import (
	"hash/fnv"
	"log"
	"plugin"
)

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MapFunc = func(string, string) []KeyValue
type ReduceFunc = func(string, []string) string

func loadPlugin(fileName string) (MapFunc, ReduceFunc) {
	p, err := plugin.Open(fileName)
	if err != nil {
		log.Fatalf("cannot load plugin %v", fileName)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", fileName)
	}
	mapf := xmapf.(MapFunc)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", fileName)
	}
	reducef := xreducef.(ReduceFunc)

	return mapf, reducef
}
