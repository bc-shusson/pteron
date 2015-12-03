package main

import (
  // "fmt"
  "io/ioutil"
  "os"
  "strconv"
  "strings"
)

func check(e error) {
  if e != nil {
    panic(e)
  }
}

func dataWorker(storeId string, results chan string) {
  results <- queryDatabase(storeId)
}

func queryDatabase(storeId string) string {
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

  for i := 0; i < len(storeIds); i++ {
    go dataWorker(storeIds[i], results)
  }

  for i := 0; i < len(storeIds); i++ {
    result := <-results
    thing := strconv.Itoa(i)
    check(err)
    _, err = f.WriteString(result + thing)
    check(err)
  }

}

func main() {
  dat, err := ioutil.ReadFile("test.data")
  check(err)
  storeIds := strings.Split(string(dat), ",")
  queryDatabases(storeIds)
}
