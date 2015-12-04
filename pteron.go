package main

import (
  "bytes"
  "database/sql"
  "encoding/csv"
  "encoding/json"
  "flag"
  "fmt"
  _ "github.com/go-sql-driver/mysql"
  "github.com/parnurzeal/gorequest"
  "io/ioutil"
  "os"
  "regexp"
  "strconv"
  "strings"
  "time"
)

const (
  QUERY_TIMEOUT              = 10
  MAXIMUM_WORKERS            = 1
  RATE_LIMIT_IN_MILLISECONDS = 1
  CONFIG_SERVICE_URL         = "http://127.0.0.1:83/"
)

func main() {
  inputStoreIds := flag.String("storeIds", "", "path to file that contains comma separated store ids")
  query := flag.String("query", "", "query to run")
  flag.Parse()
  output := flag.Args()[0]
  dat, err := ioutil.ReadFile(*inputStoreIds)
  check(err)

  storeIds := strings.Split(strings.Trim(string(dat), "\n"), ",")

  // TODO: test the *
  // if len(storeIds) == 1 && storeIds[0] == "*" {
  //   storeIds = getManagedDomains()
  // }

  jobs := createJobs(storeIds)
  results := createWorkers(storeIds, jobs, *query)
  handleResults(results, storeIds, output)
}

func dataWorker(id int, jobs <-chan string, results chan map[string]string, query string) {
  // add a rate limiter because sql on OSX can't seem to handle the amount of requests
  // can be removed on production
  limiter := time.Tick(time.Millisecond * RATE_LIMIT_IN_MILLISECONDS)
  for j := range jobs {
    <-limiter
    // create an anon go routine so we can timeout if the query takes to long
    queryJob := make(chan string, 1)
    go func() {
      fmt.Println("worker", id, "processing job", j)
      dsn := getDatabaseDsn(j)
      queryJob <- queryDatabase(dsn, query, j)
      // queryJob <- queryDatabase(j) //uncomment to run with staight dsns
    }()

    resultMap := make(map[string]string)
    select {
    case result := <-queryJob:
      resultMap["result"] = result
      resultMap["databaseDsn"] = j
      results <- resultMap
    case <-time.After(time.Second * QUERY_TIMEOUT):
      fmt.Println("worker", id, "timed out trying to process ", j)
      resultMap[j] = "query timed out"
      results <- resultMap
    }
  }
}

func queryDatabase(dbDsn string, query string, storeId string) string {
  db, err := sql.Open("mysql", dbDsn)
  check(err)
  defer db.Close()
  rows, err := db.Query(query)
  check(err)
  defer rows.Close()
  result := dumpTable(rows, storeId)
  // TODO: clean result
  return result
}

func createWorkers(storeIds []string, jobs <-chan string, query string) chan map[string]string {
  results := make(chan map[string]string, len(storeIds))
  workersCount := Min(len(storeIds), MAXIMUM_WORKERS)
  for w := 0; w < workersCount; w++ {
    go dataWorker(w, jobs, results, query)
  }
  return results
}

func createJobs(storeIds []string) chan string {
  jobs := make(chan string, len(storeIds))
  for i := 0; i < len(storeIds); i++ {
    // TODO: validation dsn before adding to the job
    jobs <- strings.Trim(storeIds[i], "\n")
  }
  close(jobs)
  return jobs
}

func handleResults(results <-chan map[string]string, storeIds []string, outputPath string) {
  err := os.RemoveAll(outputPath)
  check(err)
  f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
  check(err)
  defer f.Close()

  for i := 0; i < len(storeIds); i++ {
    result := <-results
    _, err = f.WriteString(result["result"])
    check(err)
  }
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

func getDatabaseDsns(storeIds []string) ([]string, error) {
  var dsns []string
  if storeIds == nil {
    return dsns, fmt.Errorf("no store ids given")
  }
  for i := 0; i < len(storeIds); i++ {
    dbDsn := getDatabaseDsn(storeIds[i])
    dsns = append(dsns, dbDsn)
  }
  return dsns, nil
}

func getDatabaseDsn(storeId string) string {
  request := gorequest.New()
  url := CONFIG_SERVICE_URL + "store/" + storeId
  _, body, _ := request.Get(url).
    Set("User-Agent", "bcserver").
    Set("X-Application-Key", "BCACCOUNTS").
    End()
  var parsedJson map[string]*json.RawMessage
  _ = json.Unmarshal([]byte(body), &parsedJson)
  config := make(map[string]string)
  config["dbServer"], _ = strconv.Unquote(string(*parsedJson["dbServer"]))
  config["dbUser"], _ = strconv.Unquote(string(*parsedJson["dbUser"]))
  config["dbPass"], _ = strconv.Unquote(string(*parsedJson["dbPass"]))
  config["dbDatabase"], _ = strconv.Unquote(string(*parsedJson["dbDatabase"]))

  // get the slave db - does not work in the VM because the dbserver is always localhost
  re1, err := regexp.Compile(`(((?:store-|))(db[0-9])((?:-\w|)))\.((syd1bc|dal[1,5,7]sl)\.bigcommerce.net)`)
  if err != nil {
    panic(err) // do proper error handling
  }
  result := re1.FindStringSubmatch(string(*parsedJson["dbServer"]))
  if len(result) > 0 {
    config["dbServer"] = result[3] + result[4] + "c" + result[5] + "." + result[6]
  }
  // username:password@protocol(address)/dbname?param=value
  // hack - default port
  config["dbServer"] = fmt.Sprintf("%s:3306", config["dbServer"])
  dbDsn := createDbDsn(config["dbUser"], config["dbPass"], config["dbServer"], config["dbDatabase"])
  return dbDsn
}

func createDbDsn(user string, pwd string, server string, database string) string {
  return fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pwd, server, database)
}

func getManagedDomains() []string {
  // untested
  dat, err := ioutil.ReadFile("/etc/psa/.psa.shadow")
  check(err)
  password := strings.Trim(string(dat), " ")
  dbDsn := createDbDsn("admin", password, "localhost:3306", "psa")
  db, err := sql.Open("mysql", dbDsn)
  check(err)
  defer db.Close()
  var (
    id   int
    name string
  )
  var storeIds []string
  rows, err := db.Query("SELECT d.id, d.name FROM domains d JOIN hosting h ON (h.dom_id = d.id)")
  check(err)
  defer rows.Close()
  for rows.Next() {
    err := rows.Scan(&id, &name)
    check(err)
    storeIds = append(storeIds, strconv.Itoa(id))
  }
  return storeIds
}
