package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"net/http"
	"time"
)

const (
	username = "root"
	password = "password"
	hostname = "127.0.0.1"
	port     = 5432
	database = "postgres"
)

var redisClient *redis.Client
var db *sql.DB

type Product struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Price    string `json:"price"`
	Category string `json:"category"`
}

func connectRedis() error {
	redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	_, err := redisClient.Ping(context.Background()).Result()
	return err
}

func connectDB() error {
	DSN := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", username, password, hostname, port, database)
	var err error
	db, err = sql.Open("postgres", DSN)
	if err != nil {
		return err
	}
	err = db.Ping()
	return err
}

func addProductHand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	err1 := r.ParseForm()
	if err1 != nil {
		http.Error(w, "Error parsing form", http.StatusBadRequest)
		return
	}
	product := &Product{
		Name:     r.Form.Get("name"),
		Price:    r.Form.Get("price"),
		Category: r.Form.Get("category"),
	}
	err2 := addProductToDatabase(product)
	if err2 != nil {
		return
	}
}

func getProduct(r *http.Request) (*Product, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}
	productId := r.Form.Get("id")
	cacheKey := fmt.Sprintf("product%s", productId)
	cacheResult, err := redisClient.Get(context.Background(), cacheKey).Bytes()

	var product Product
	if err != nil {
		product, err := getProductById(productId)
		if err != nil {
			return nil, err
		}

		jsonData, err := json.Marshal(product)
		if err != nil {
			return nil, err
		}

		err = redisClient.Set(context.Background(), cacheKey, jsonData, 100*time.Second).Err() // Set expiry to 5 minutes
		if err != nil {
			return nil, err
		}
		return product, nil
	} else {
		err := json.Unmarshal(cacheResult, &product)
		if err != nil {
			return nil, err
		}
		return &product, nil
	}
}

func getProductHand(w http.ResponseWriter, r *http.Request) {
	product, err := getProduct(r)
	if err != nil {
		return
	}
	if product == nil {
		return
	}
	jsonData, err := json.Marshal(product)
	if err != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}
func getProductById(id string) (*Product, error) {
	query := fmt.Sprintf("SELECT * FROM products WHERE productId = %s", id)
	row := db.QueryRow(query)
	product := &Product{}
	err := row.Scan(&product.ID, &product.Name, &product.Price, &product.Category)
	if err != nil {
		return nil, err
	}
	return product, nil
}

func addProductToDatabase(p *Product) error {
	var _, err = db.Exec("INSERT INTO products(name, price, category) VALUES ($1, $2, $3)", p.Name, p.Price, p.Category)
	return err
}

func main() {
	err := connectDB()
	if err != nil {
		return
	}
	err = connectRedis()
	if err != nil {
		return
	}
	http.HandleFunc("/product", getProductHand)
	http.HandleFunc("/product-add", addProductHand)

	fmt.Println("Server is running on port 8080...")
	_ = http.ListenAndServe(":8080", nil)
}
