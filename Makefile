build:
	go build -o bin/

execute:
	.\bin\1brc-go.exe 

run:
	make build
	make execute