package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/joho/godotenv"
)

var esClient *elasticsearch.TypedClient

type esData struct {
	Name    string `json:"name"`
	Number  int    `json:"number"`
	Payload string `json:"payload"`
}

var (
	workerMu  sync.Mutex
	counter   atomic.Int64
	rateCh    chan int
	stopCh    chan struct{}
	doneCh    chan struct{}
	running   bool
	workerIdx string
)

func setupElasticSearchConnection() (*elasticsearch.TypedClient, error) {
	if err := godotenv.Load(); err != nil {
		log.Printf("no .env file loaded: %v", err)
	}

	addr := os.Getenv("ELASTICSEARCH_URL")
	if addr == "" {
		addr = "http://localhost:9200"
	}

	cfg := elasticsearch.Config{
		Addresses: []string{addr},
		Username:  os.Getenv("ELASTICSEARCH_USERNAME"),
		Password:  os.Getenv("ELASTICSEARCH_PASSWORD"),
		APIKey:    os.Getenv("ELASTICSEARCH_API_KEY"),
	}

	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, lastErr = client.Info().Do(ctx)
		cancel()
		if lastErr == nil {
			return client, nil
		}
		log.Printf("elasticsearch not ready (attempt %d/30): %v", attempt, lastErr)
		time.Sleep(5 * time.Second)
	}
	return nil, lastErr
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func createIndex(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
	if id == "" {
		writeErr(w, http.StatusUnprocessableEntity, "id cannot be NULL")
		return
	}

	res, err := esClient.Indices.Create(id).Do(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"acknowledged": res.Acknowledged,
		"index":        res.Index,
	})
}

func indexOne(ctx context.Context, index string) {
	n := counter.Add(1)
	payload := ""
	switch {
	case n%15 == 0:
		payload = "fizzbuzz"
	case n%3 == 0:
		payload = "fizz"
	case n%5 == 0:
		payload = "buzz"
	}
	doc := esData{Name: "testlog", Number: int(n), Payload: payload}
	if _, err := esClient.Index(index).Document(doc).Do(ctx); err != nil {
		log.Printf("index err n=%d: %v", n, err)
	}
}

func runWorker(index string, initialRate int) {
	defer close(doneCh)
	ticker := time.NewTicker(time.Second / time.Duration(initialRate))
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case newRate := <-rateCh:
			if newRate > 0 {
				ticker.Reset(time.Second / time.Duration(newRate))
			}
		case <-ticker.C:
			indexOne(context.Background(), index)
		}
	}
}

func startWorker(w http.ResponseWriter, r *http.Request) {
	index := r.FormValue("index")
	rateStr := r.FormValue("rate")
	if index == "" {
		writeErr(w, http.StatusUnprocessableEntity, "index cannot be NULL")
		return
	}
	rate, err := strconv.Atoi(rateStr)
	if err != nil || rate <= 0 {
		writeErr(w, http.StatusUnprocessableEntity, "rate must be positive int")
		return
	}

	workerMu.Lock()
	defer workerMu.Unlock()
	if running {
		writeErr(w, http.StatusConflict, "worker already running")
		return
	}

	rateCh = make(chan int, 1)
	stopCh = make(chan struct{})
	doneCh = make(chan struct{})
	workerIdx = index
	running = true
	go runWorker(index, rate)

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "started",
		"index":  index,
		"rate":   rate,
	})
}

func updateRate(w http.ResponseWriter, r *http.Request) {
	rateStr := r.FormValue("rate")
	rate, err := strconv.Atoi(rateStr)
	if err != nil || rate <= 0 {
		writeErr(w, http.StatusUnprocessableEntity, "rate must be positive int")
		return
	}

	workerMu.Lock()
	defer workerMu.Unlock()
	if !running {
		writeErr(w, http.StatusConflict, "worker not running")
		return
	}

	select {
	case <-rateCh:
	default:
	}
	rateCh <- rate

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "rate updated",
		"rate":   rate,
	})
}

func stopWorker(w http.ResponseWriter, r *http.Request) {
	workerMu.Lock()
	defer workerMu.Unlock()
	if !running {
		writeErr(w, http.StatusConflict, "worker not running")
		return
	}

	close(stopCh)
	<-doneCh
	running = false

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "stopped",
		"counter": counter.Load(),
	})
}

func workerStatus(w http.ResponseWriter, r *http.Request) {
	workerMu.Lock()
	defer workerMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"running": running,
		"index":   workerIdx,
		"counter": counter.Load(),
	})
}

func queryDocuments(w http.ResponseWriter, r *http.Request) {
	index := r.FormValue("index")
	if index == "" {
		writeErr(w, http.StatusUnprocessableEntity, "index cannot be NULL")
		return
	}
	payload := r.FormValue("payload")

	size := 10
	if s := r.FormValue("size"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusUnprocessableEntity, "size must be positive int")
			return
		}
		if n > 1000 {
			n = 1000
		}
		size = n
	}

	req := &search.Request{Size: &size}
	if payload != "" {
		req.Query = &types.Query{
			Match: map[string]types.MatchQuery{
				"payload": {Query: payload},
			},
		}
	}

	res, err := esClient.Search().Index(index).Request(req).Do(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	docs := make([]esData, 0, len(res.Hits.Hits))
	for _, h := range res.Hits.Hits {
		var d esData
		if err := json.Unmarshal(h.Source_, &d); err != nil {
			log.Printf("unmarshal hit: %v", err)
			continue
		}
		docs = append(docs, d)
	}

	total := int64(0)
	if res.Hits.Total != nil {
		total = res.Hits.Total.Value
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": total,
		"hits":  docs,
	})
}

func main() {
	client, err := setupElasticSearchConnection()
	if err != nil {
		log.Fatalf("elasticsearch connect: %v", err)
	}
	esClient = client

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /index", createIndex)
	mux.HandleFunc("POST /documents/start", startWorker)
	mux.HandleFunc("POST /documents/rate", updateRate)
	mux.HandleFunc("POST /documents/stop", stopWorker)
	mux.HandleFunc("GET /documents/status", workerStatus)
	mux.HandleFunc("GET /documents/query", queryDocuments)

	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
