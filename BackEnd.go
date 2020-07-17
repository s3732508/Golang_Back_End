package main

import (
	"archive/tar"
	"compress/bzip2"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	//"reflect"
	"encoding/json"
	"log"
	"net/http"
)

type Order struct {
	Order_name       string
	Customer_Company string
	Customer_Name    string
	Order_Date       int64
	Delivered_Amount string
	Total_Amount     string
	Products []string
}

// PostGreSql credentials
const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "Motorola9"
	dbname   = "postgres"
)

//MongoDB Credentials
const MongoDBClientURI = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false"

const MongoDBD_DatabaseName = "local"


func main() {

	tarFile := extract_TAR_from_BZ2("data.tar.bz2", "./")
	extract_Files_from_TAR(tarFile, "data")
	os.Remove(tarFile)

	client, err := mongo.NewClient(options.Client().ApplyURI(MongoDBClientURI))
	if err != nil {
		fmt.Println(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		fmt.Println(err)
	}
	defer client.Disconnect(ctx)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		fmt.Println(err)
	}
	Database := client.Database(MongoDBD_DatabaseName)

	Load_CSV_into_MongoDB("./data/Test task - Mongo - customer_companies.csv", Database, ctx)
	Load_CSV_into_MongoDB("./data/Test task - Mongo - customers.csv", Database, ctx)

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)
	defer db.Close()
	err = db.Ping()
	CheckError(err)
	Load_CSV_into_PostGreSQL("./data/Test task - Postgres - deliveries.csv", db)
	Load_CSV_into_PostGreSQL("./data/Test task - Postgres - order_items.csv", db)
	Load_CSV_into_PostGreSQL("./data/Test task - Postgres - orders.csv", db)

	handler := HttpHandler{}

	// Port Number
	http.ListenAndServe(":9000", handler)

}

type HttpHandler struct {
}

func (h HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	
	res.Header().Set("Content-Type", "text/html; charset=utf-8")
	res.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(res).Encode(AllOrders())
}

func Load_CSV_into_MongoDB(fileName string, Database *mongo.Database, ctx context.Context) {
	baseFileName := filepath.Base(fileName)
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	collection := Database.Collection(justFileName)

	if err := collection.Drop(ctx); err != nil {
		fmt.Println(err)
	}

	fmt.Println("File being read->\t\t\t", baseFileName)
	fileReader, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer fileReader.Close()
	CsvReader := csv.NewReader(fileReader)
	i := 0
	var keys []string

	for {
		record, err := CsvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		if i == 0 {
			keys = record
			i++
		} else {
			for i := 0; i < len(record); i++ {
				if i == 0 {
					_, err = collection.InsertOne(ctx, bson.D{
						{keys[i], record[i]},
					})

				} else {
					_, _ = collection.UpdateOne(
						ctx,
						bson.M{keys[0]: record[0]},
						bson.D{
							{"$set", bson.D{{keys[i], record[i]}}},
						},
					)
				}

			}
		}
	}
}

func AllOrders() []Order {

	baseFileName := filepath.Base("Test task - Postgres - orders.csv")
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	TableName := strings.ReplaceAll(strings.ReplaceAll(justFileName, " ", ""), "-", "")
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)
	defer db.Close()
	err = db.Ping()
	CheckError(err)

	client, err := mongo.NewClient(options.Client().ApplyURI(MongoDBClientURI))
	if err != nil {
		fmt.Println(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		fmt.Println(err)
	}
	defer client.Disconnect(ctx)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		fmt.Println(err)
	}
	Database := client.Database(MongoDBD_DatabaseName)

	data, err := db.Query(fmt.Sprintf("select id,created_at, order_name,customer_id from %s", TableName))
	if err != nil {
		fmt.Println(err)
	}

	length := SizeofOrders(db)
	Orders := make([]Order, length)

	i := 0
	for data.Next() {
		var id string
		var created_at string
		var order_name string
		var customer_id string

		data.Scan(&id, &created_at, &order_name, &customer_id)

		t, _ := time.Parse(time.RFC3339, created_at)

		array := getAmount(id, db)
		Orders[i] = Order{
			Order_name:       order_name,
			Customer_Company: getCustomerCompany(customer_id, Database, ctx),
			Customer_Name:    getCustomerNameandID(customer_id, "name", Database, ctx),
			Order_Date:       t.Unix(),
			Delivered_Amount: array[1],
			Total_Amount:     array[0],
			Products: getProducts(id, db),
		}
		i++
		

	}
	fmt.Println("Request Received")
	//Orders_enc, err := json.Marshal(Orders)

	return Orders

}

func SizeofOrders(db *sql.DB) int {
	baseFileName := filepath.Base("Test task - Postgres - orders.csv")
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	TableName := strings.ReplaceAll(strings.ReplaceAll(justFileName, " ", ""), "-", "")

	data, err := db.Query(fmt.Sprintf("select id,created_at, order_name,customer_id from %s", TableName))
	if err != nil {
		fmt.Println(err)
	}
	count := 0

	for data.Next() {
		count++
	}
	return count
}

func getAmount(Order_id string, db *sql.DB) []string {
	baseFileName := filepath.Base("Test task - Postgres - order_items.csv")
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	TableName := strings.ReplaceAll(strings.ReplaceAll(justFileName, " ", ""), "-", "")

	data, err := db.Query(fmt.Sprintf("select id,order_id,price_per_unit,quantity,product from %s where order_id=%s%s%s", TableName, "'", Order_id, "'"))
	if err != nil {
		fmt.Println(err)
	}
	var Total_Amount float64
	var Delivered_Amount float64
	array := make([]string, 2)
	Delivered_Amount = 0.0
	Total_Amount = 0.0
	for data.Next() {
		var id string

		var order_id string
		var price_per_unit string
		var quantity string
		var product string

		data.Scan(&id, &order_id, &price_per_unit, &quantity, &product)
		quantity_float, _ := strconv.ParseFloat(quantity, 32)
		price_per_unit_float, _ := strconv.ParseFloat(price_per_unit, 32)
		//fmt.Println(reflect.TypeOf(quantity_float))
		//fmt.Println(reflect.TypeOf(Total_Amount))
		delivered_quantity_float, _ := strconv.ParseFloat(getDeliveredQuantity(id, db), 32)
		//fmt.Println(quantity_float*price_per_unit_float)
		Total_Amount = Total_Amount + quantity_float*price_per_unit_float
		//fmt.Print("\t",getDeliveredQuantity(id))
		Delivered_Amount = Delivered_Amount + delivered_quantity_float*price_per_unit_float
	}

	Total := fmt.Sprintf("%.2f", Total_Amount)
	if Total == "0.00" {
		array[0] = "-"
	} else {
		array[0] = Total

	}

	Delivered := fmt.Sprintf("%.2f", Delivered_Amount)

	if Delivered == "0.00" {
		array[1] = "-"
	} else {
		array[1] = Delivered
	}

	return array
}


func getProducts(Order_id string, db *sql.DB) []string {
	baseFileName := filepath.Base("Test task - Postgres - order_items.csv")
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	TableName := strings.ReplaceAll(strings.ReplaceAll(justFileName, " ", ""), "-", "")

	data, err := db.Query(fmt.Sprintf("select id,order_id,price_per_unit,quantity,product from %s where order_id=%s%s%s", TableName, "'", Order_id, "'"))
	if err != nil {
		fmt.Println(err)
	}

	var array []string
	
	for data.Next() {
		var id string

		var order_id string
		var price_per_unit string
		var quantity string
		var product string

		data.Scan(&id, &order_id, &price_per_unit, &quantity, &product)
		 array = append(array, product)
	}

	
	return array
}

func getDeliveredQuantity(Order_item_id string, db *sql.DB) string {

	baseFileName := filepath.Base("Test task - Postgres - deliveries.csv")
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	TableName := strings.ReplaceAll(strings.ReplaceAll(justFileName, " ", ""), "-", "")

	data, err := db.Query(fmt.Sprintf("select id,order_item_id,delivered_quantity from %s where order_item_id=%s%s%s", TableName, "'", Order_item_id, "'"))
	if err != nil {
		fmt.Println(err)
	}
	var quantity int64
	quantity = 0
	for data.Next() {
		var id string

		var order_item_id string
		var delivered_quantity string

		data.Scan(&id, &order_item_id, &delivered_quantity)

		//fmt.Print("\t"+delivered_quantity)
		integer, _ := strconv.ParseInt(delivered_quantity, 10, 64)
		quantity = quantity + integer

	}

	s := fmt.Sprintf("%d", quantity) // s == "123.456000"

	return s
}

func getCustomerCompany(Customer_id string, Database *mongo.Database, ctx context.Context) string {
	Company_id := getCustomerNameandID(Customer_id, "company_id", Database, ctx)
	type Company struct {
		Company_id   string
		Company_name string
	}

	collection := Database.Collection("Test task - Mongo - customer_companies")

	var result Company

	filter := bson.D{{"company_id", Company_id}}
	err := collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	return result.Company_name
}

func getCustomerNameandID(Customer_id string, option string, Database *mongo.Database, ctx context.Context) string {

	type Customer struct {
		Name         string
		User_id      string
		Password     string
		Company_id   string
		Credit_cards string
		Products []string
	}

	collection := Database.Collection("Test task - Mongo - customers")

	var result Customer

	filter := bson.D{{"user_id", Customer_id}}
	err := collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	if option == "name" {
		return result.Name
	}
	return result.Company_id
}

func extract_TAR_from_BZ2(fileName string, destination string) string {
	fmt.Println("File being uncompressed->\t\t", fileName, "\nDestination of tar file->\t\t", destination)
	fileReader, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer fileReader.Close()
	archive := bzip2.NewReader(fileReader)
	newFileName := fileName[0 : len(fileName)-len(filepath.Ext(fileName))]
	var target = filepath.Join(destination, newFileName)
	writer, err := os.Create(target)
	if err != nil {
		fmt.Println(err)
	}
	defer writer.Close()
	_, err = io.Copy(writer, archive)
	return newFileName
}

func extract_Files_from_TAR(fileName string, destination string) {
	fmt.Println("File being uncompressed->\t\t", fileName, "\nDestination of Excel files->\t\t", destination)
	fileReader, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer fileReader.Close()
	archive := tar.NewReader(fileReader)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		os.Mkdir(destination, 0707)
		var target = filepath.Join("./"+destination+"/", header.Name)
		writer, err := os.Create(target)
		if err != nil {
			fmt.Println(err)
		}
		defer writer.Close()
		_, err = io.Copy(writer, archive)
	}
}



func Load_CSV_into_PostGreSQL(fileName string, db *sql.DB) {

	baseFileName := filepath.Base(fileName)
	justFileName := baseFileName[0 : len(baseFileName)-len(filepath.Ext(baseFileName))]

	fmt.Println("File being read->\t\t\t", baseFileName)
	fileReader, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer fileReader.Close()
	CsvReader := csv.NewReader(fileReader)
	i := 0
	var keys []string

	for {
		record, err := CsvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		// Removes Spaces and Dashes
		TableName := strings.ReplaceAll(strings.ReplaceAll(justFileName, " ", ""), "-", "")

		if i == 0 {
			keys = record
			db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", TableName))
			_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s()", TableName))
			if err != nil {
				fmt.Println(err)
			}

			for j := 0; j < len(record); j++ {
				_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s VARCHAR(50)", TableName, record[j]))
				if err != nil {
					fmt.Println(err)
				}
			}
			i++
		} else {
			for k := 0; k < len(record); k++ {
				if k == 0 {

					_, err = db.Exec(fmt.Sprintf("INSERT INTO %s(%s) VALUES ($1)", TableName, keys[k]), record[k])
					if err != nil {
						fmt.Println(err)
					}

				} else {
					_, err = db.Exec(fmt.Sprintf("update %s set %s=$1 where %s = $2", TableName, keys[k], keys[0]), record[k], record[0])
					if err != nil {
						fmt.Println(err)
					}
				}

			}
		}

		CheckError(err)
	}
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
