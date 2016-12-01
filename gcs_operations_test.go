package gcs_operation

import (
  "fmt"
  //"os"
  //"path/filepath"
  "testing"

  //"golang.org/x/net/context"
  //"golang.org/x/oauth2/google"
  //storage "google.golang.org/api/storage/v1"
)

func TestBucketCreation(t *testing.T) {
  bucket_name := "test-bucket-gcs-operations"
  result := CreateBucket("mlab-oti", bucket_name)
  if result == false {
    t.Errorf("Cannot create bucket")
    return
  }

  result = CopyOneFile("tarfile_raw_data", bucket_name, "search.png")
  if result == false {
    t.Errorf("Cannot copy file from another bucket.")
    return
  }

  file_names := GetFileNamesFromBucket(bucket_name)

  fmt.Printf("Files in bucket %v:\n", bucket_name)
  for _, file_name := range file_names {
    fmt.Println(file_name)
  }

  result = DeleteFiles(bucket_name, "")
  if result == false {
    t.Errorf("Cannot delete bucket")
    return
  } 
}

