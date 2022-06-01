package server

import (
	"encoding/json"
	"net/http"

	"github.com/brianseitel/shard/internal/shard"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type Controller struct {
	Logger  *zap.Logger
	ShardDB *shard.Shard
}

func (c *Controller) Register(router *mux.Router) {
	router.HandleFunc("/v1/database/{key}", c.Get()).Methods(http.MethodGet)
	router.HandleFunc("/v1/database/{key}", c.Delete()).Methods(http.MethodDelete)
	router.HandleFunc("/v1/database", c.List()).Methods(http.MethodGet)
	router.HandleFunc("/v1/database", c.Put()).Methods(http.MethodPut)
}

func (c *Controller) List() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := c.ShardDB.ListKeys()

		j, _ := json.MarshalIndent(result, "", "    ")

		w.WriteHeader(http.StatusOK)
		w.Write(j)
	}
}

func (c *Controller) Get() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		key, _ := vars["key"]

		c.ShardDB.Mutex.Lock()
		result := c.ShardDB.Get(key)
		c.ShardDB.Mutex.Unlock()

		if result == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		j, _ := json.MarshalIndent(result, "", "    ")

		w.WriteHeader(http.StatusOK)
		w.Write(j)
	}
}

type Request struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func (c *Controller) Put() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body Request
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			panic(err)
		}
		defer r.Body.Close()

		c.ShardDB.Mutex.Lock()
		err = c.ShardDB.Set(body.Key, body.Value)
		c.ShardDB.Mutex.Unlock()
		if err != nil {
			panic(err)
		}

		w.WriteHeader(http.StatusOK)
	}
}

func (c *Controller) Delete() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		key, _ := vars["key"]

		c.ShardDB.Mutex.Lock()
		c.ShardDB.Delete(key)
		c.ShardDB.Mutex.Unlock()

		w.WriteHeader(http.StatusOK)
	}
}
