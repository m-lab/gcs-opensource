/*
Copyright 2013 Google Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
	http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gcs_operation

import (
  "fmt"
  "testing"
)

// Unittest
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

