cover:
	go test -coverprofile=cover.coverprofile .
	go tool cover -html=cover.coverprofile
