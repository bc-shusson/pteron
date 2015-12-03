package main

import (
  "fmt"
  "io/ioutil"
  "os"
  "strconv"
  "strings"
  "time"
)

const QUERY_TIMEOUT = 10
const WORKER_COUNT = 100000

func check(e error) {
  if e != nil {
    panic(e)
  }
}

func dataWorker(id int, jobs <-chan string, results chan string) {
  for j := range jobs {
    // create an anon go routine so we can timeout if the query takes to long
    queryJob := make(chan string, 1)
    go func() {
      fmt.Println("worker", id, "processing job", j)
      queryJob <- queryDatabase(j)
    }()

    select {
    case result := <-queryJob:
      results <- result
    case <-time.After(time.Second * QUERY_TIMEOUT):
      fmt.Println("worker", id, "timed out trying to process ", j)
      results <- "Query Timed out"
    }
  }
}

func queryDatabase(storeId string) string {
  if storeId == "2601" {
    time.Sleep(time.Second * 20)
  }
  time.Sleep(time.Second * 1)
  return "A RESULT"
}

func queryDatabases(storeIds []string) {
  path := "query.results"
  err := os.RemoveAll(path)
  check(err)
  f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
  check(err)
  defer f.Close()
  results := make(chan string, len(storeIds))

  jobs := make(chan string, len(storeIds))

  for w := 0; w < WORKER_COUNT; w++ {
    go dataWorker(w, jobs, results)
  }

  for i := 0; i < len(storeIds); i++ {
    jobs <- storeIds[i]
  }
  close(jobs)

  for i := 0; i < len(storeIds); i++ {
    result := <-results
    index := strconv.Itoa(i)
    check(err)
    _, err = f.WriteString(result + index)
    check(err)
  }

}

func main() {
  dat, err := ioutil.ReadFile("test.data")
  check(err)
  storeIds := strings.Split(string(dat), ",")
  queryDatabases(storeIds)
}
