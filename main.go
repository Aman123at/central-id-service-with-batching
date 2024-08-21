package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

const (
	OrderService   string = "orders"
	PaymentService string = "payments"
	MailService    string = "mail"
)

type IdRange struct {
	From int
	To   int
}

func generateID(service string, batchSize int) (IdRange, error) {
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/centralid")
	if err != nil {
		return IdRange{}, err
	}
	defer db.Close()
	var idRange IdRange

	// check what is the current id count for requested service in db
	var currIdCount int
	fetchQuery := fmt.Sprintf("SELECT counter FROM genid WHERE service='%s' FOR UPDATE", service)
	err = db.QueryRow(fetchQuery).Scan(&currIdCount)
	if err != nil {
		return IdRange{}, err
	}
	res, updateErr := db.Exec("UPDATE genid SET counter=? WHERE service=?", currIdCount+batchSize, service)
	if updateErr != nil {
		return IdRange{}, err
	}
	rowsAffected, rowErr := res.RowsAffected()
	if rowErr != nil {
		return IdRange{}, err
	}
	if rowsAffected > 0 {
		// increment the id count by batch size
		idRange.From = currIdCount + 1
		idRange.To = currIdCount + batchSize
	}
	return idRange, nil
}

func handleGenerateId(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()

	// extract batch size from request query
	batch := q.Get("batch")

	// convert batch size to integer
	batchInt, err := strconv.Atoi(batch)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid batch number"})
		return
	}

	params := mux.Vars(r)

	serviceName := params["service"]

	if serviceName == OrderService || serviceName == PaymentService || serviceName == MailService {
		// get generated ID
		idRange, genErr := generateID(serviceName, batchInt)

		if genErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to generate ID"})
		}

		// send ID range in response
		json.NewEncoder(w).Encode(map[string]IdRange{
			"idRange": idRange,
		})
		return
	} else {
		w.WriteHeader(http.StatusBadRequest)

		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid service name",
		})
		return
	}
}

func main() {
	log.Println("Central ID generation service")
	router := mux.NewRouter()
	router.HandleFunc("/generateID/{service}", handleGenerateId).Methods("GET")
	log.Println("Server starting on port : 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}
