param([string]$cmd = "help")
switch ($cmd) {
	"up" { docker compose -f deploy/docker-compose.yml up -d }
	"down" { docker compose -f deploy/docker-compose.yml down }
	"ps" { docker compose -f deploy/docker-compose.yml ps }
	"importer" { go run ./cmd/importer }
	"worker" { go run ./cmd/catalog-worker }
	"test" { go test ./... }
	default { "usage: .\dev.ps1 up|down|ps|importer|worker|test" }
}