#/bin/bash
mkdir -p bin
rm -rf bin/*
go build -race -o bin/coordinator
go build -race -o bin/worker


mkdir -p lib
rm -rf lib/*

for dir in plugins/*;
do
    plugin_name=$(basename "$dir")
    go build -o lib/$plugin_name.so -race -buildmode=plugin $dir/$plugin_name.go
done