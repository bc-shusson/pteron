package main

import (
  "bytes"
  "database/sql"
  "encoding/csv"
  "fmt"
  _ "github.com/go-sql-driver/mysql"
  "io/ioutil"
  "os"
  "strings"
  "time"
)

const QUERY_TIMEOUT = 10
const MAXIMUM_WORKERS = 2
const RATE_LIMIT_IN_MILLISECONDS = 2

func check(e error) {
  if e != nil {
    panic(e)
  }
}

func Min(x, y int) int {
  if x < y {
    return x
  }
  return y
}

func dataWorker(id int, jobs <-chan string, results chan map[string]string) {
  // add a rate limiter because sql on OSX can't seem to handle the amount of requests
  // can be removed on production
  limiter := time.Tick(time.Millisecond * 10)
  for j := range jobs {
    <-limiter
    // create an anon go routine so we can timeout if the query takes to long
    queryJob := make(chan string, 1)
    go func() {
      fmt.Println("worker", id, "processing job", j)
      queryJob <- queryDatabase(j)
    }()

    resultMap := make(map[string]string)
    select {
    case result := <-queryJob:
      resultMap["result"] = result
      resultMap["store_id"] = j
      results <- resultMap
    case <-time.After(time.Second * QUERY_TIMEOUT):
      fmt.Println("worker", id, "timed out trying to process ", j)
      resultMap[j] = "query timed out"
      results <- resultMap
    }
  }
}

func queryDatabase(storeId string) string {
  db, err := sql.Open("mysql", "/test")
  check(err)
  defer db.Close()
  rows, err := db.Query("SELECT * FROM products")
  check(err)
  defer rows.Close()
  result := dumpTable(rows, storeId)
  return result
}

func createWorkers(storeIds []string, jobs <-chan string) chan map[string]string {
  results := make(chan map[string]string, len(storeIds))
  workersCount := Min(len(storeIds), MAXIMUM_WORKERS)
  for w := 0; w < workersCount; w++ {
    go dataWorker(w, jobs, results)
  }
  return results
}

func createJobs(storeIds []string) chan string {
  jobs := make(chan string, len(storeIds))
  for i := 0; i < len(storeIds); i++ {
    jobs <- storeIds[i]
  }
  close(jobs)
  return jobs
}

func handleResults(results <-chan map[string]string, storeIds []string) {

  path := "query.results"
  err := os.RemoveAll(path)
  check(err)
  f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
  check(err)
  defer f.Close()

  for i := 0; i < len(storeIds); i++ {
    result := <-results
    _, err = f.WriteString(result["result"])
    check(err)
  }
}

func queryDatabases(storeIds []string) {

  jobs := createJobs(storeIds)
  results := createWorkers(storeIds, jobs)

  handleResults(results, storeIds)

}

func main() {
  dat, err := ioutil.ReadFile("test0.data")
  check(err)
  storeIds := strings.Split(string(dat), ",")
  queryDatabases(storeIds)
}

// adapted from https://github.com/go-sql-driver/mysql/wiki/Examples#rawbytes
func dumpTable(rows *sql.Rows, storeId string) string {
  buf := new(bytes.Buffer)
  writer := csv.NewWriter(buf)
  writer.Comma = '\t'
  // Get column names
  columns, err := rows.Columns()
  // writer.Write(append([]string{"storeId"}, columns...))
  if err != nil {
    panic(err.Error()) // proper error handling instead of panic in your app
  }

  // Make a slice for the values
  values := make([]sql.RawBytes, len(columns))

  // rows.Scan wants '[]interface{}' as an argument, so we must copy the
  // references into such a slice
  // See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
  scanArgs := make([]interface{}, len(values))
  for i := range values {
    scanArgs[i] = &values[i]
  }

  // Fetch rows
  for rows.Next() {
    // get RawBytes from data
    err = rows.Scan(scanArgs...)
    if err != nil {
      panic(err.Error()) // proper error handling instead of panic in your app
    }
    // Now do something with the data.
    var results = make([]string, 0)
    var value string
    for _, col := range values {
      // Here we can check if the value is nil (NULL value)
      if col == nil {
        value = "NULL"
      } else {
        // TODO convert to the correct type
        value = string(col)
      }
      results = append(results, value)
    }
    writer.Write(append([]string{storeId}, results...))

  }
  if err = rows.Err(); err != nil {
    panic(err.Error()) // proper error handling instead of panic in your app
  }
  writer.Flush()
  return buf.String()
}
