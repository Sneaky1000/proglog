package main

import (
	"log"

	"github.com/Sneaky1000/proglog/internal/server"
)

func main() {
	// Create the HTTP server on localhost:8080
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
