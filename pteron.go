package main

import (
  "fmt"
  "io/ioutil"
  "os"
  "strings"
  "time"
)

const QUERY_TIMEOUT = 10
const MAXIMUM_WORKERS = 10000

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
  for j := range jobs {
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
  if storeId == "2601" {
    time.Sleep(time.Second * 20)
  }
  time.Sleep(time.Second * 1)
  return "A RESULT"
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

func handeResults(results <-chan map[string]string, storeIds []string) {

  path := "query.results"
  err := os.RemoveAll(path)
  check(err)
  f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
  check(err)
  defer f.Close()

  for i := 0; i < len(storeIds); i++ {
    result := <-results
    _, err = f.WriteString(result["store_id"] + ": " + result["result"])
    check(err)
  }
}

func queryDatabases(storeIds []string) {

  jobs := createJobs(storeIds)
  results := createWorkers(storeIds, jobs)

  handeResults(results, storeIds)

}

func main() {
  dat, err := ioutil.ReadFile("test.data")
  check(err)
  storeIds := strings.Split(string(dat), ",")
  queryDatabases(storeIds)
}
