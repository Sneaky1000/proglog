package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

/*
Each handler in a JSON/HTTP Go server consinsists of 3 steps:
1. Unmarshal the request's JSON body into a struct.
2. Run that endpoint's logic with the request to obtain a result.
3. Marshal and write that result to the response.
*/

// NewHTTPServer takes in an address for the server to run on and
// returns an *http.Server.
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type httpServer struct {
	Log *Log
}

// This contains the record that the caller of the API wants appened
// to the log.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// This tells the caller what offset the log stored the records under.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// This specifies which records the caller of the API wants to read.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// This sends the records back to the caller.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

// This method implements the three steps for handlers listed above.
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	off, err := s.Log.Append(req.Record)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
}

// This method calls Read(offset uint64) to get the record stored in the log.
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	record, err := s.Log.Read(req.Offset)

	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
}
