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

// Check the content of source bucket, and sync them to destination bucket.

package main

import (
  "flag"
  "fmt"
  "os"

  "golang.org/x/net/context"
  "golang.org/x/oauth2/google"
  storage "google.golang.org/api/storage/v1"
)

const (
  // This scope allows the application full control over resources in Google Cloud Storage
  scope = storage.DevstorageFullControlScope
)

var (
  DestProjectID  = flag.String("dest_project", "mlab-oti", "The cloud project ID.")
  DestBucketName = flag.String("dest_bucket", "mlab_bigstore", "The name of destination bucket within your project.")
  SourceBucketName = flag.String("source_bucket", "m-lab", "The name of bucket for source files.")
  PrefixFileName = flag.String("prefix", "", "prefix string for files")
)

func main() {
  flag.Parse()
  if *DestBucketName == "" {
    fmt.Printf("Destination Bucket argument is required. See --help.\n")
    os.Exit(1)
  }
  if *SourceBucketName == "" {
    fmt.Printf("Source Bucket argument is required. See --help.\n")
    os.Exit(1)
  }
  if *DestProjectID == "" {
    fmt.Printf("Project argument is required. See --help.\n")
    os.Exit(1)
  }

  // Authentication is provided by the gcloud tool when running locally, and
  // by the associated service account when running on Compute Engine.
  client, err := google.DefaultClient(context.Background(), scope)
  if err != nil {
    fmt.Printf("Unable to get default client: %v \n", err)
    os.Exit(1)
  }
  service, err := storage.New(client)
  if err != nil {
    fmt.Printf("Unable to create storage service: %v\n", err)
    os.Exit(1)
  }

  // Check whether the destination bucket already exists. If not, create one.
  if _, err := service.Buckets.Get(*DestBucketName).Do(); err == nil {
    fmt.Printf("Bucket %s already exists.\n", *DestBucketName)
  } else {
    // Create a bucket.
    if res, err := service.Buckets.Insert(*DestProjectID, &storage.Bucket{Name: *DestBucketName}).Do(); err == nil {
      fmt.Printf("Created bucket %v at location %v\n", res.Name, res.SelfLink)
    } else {
      fmt.Printf("Failed creating bucket %s: %v\n", *DestBucketName, err)
      os.Exit(1)
    }
  }

  // Build list of exisitng files in destination bucket.
  existing_filenames:= make(map[string]bool)
  destPageToken := ""
  for {
    destination_files := service.Objects.List(*DestBucketName)
    if destPageToken != "" {
      destination_files.PageToken(destPageToken)
    }
    destination_files.Prefix(*PrefixFileName)
    destination_files_list, err := destination_files.Context(context.Background()).Do()
    if err != nil {
      fmt.Printf("Objects.List failed: %v\n", err)
      os.Exit(1)
    }
    for _, OneItem := range destination_files_list.Items {
      existing_filenames[OneItem.Name] = true
    }
    destPageToken = destination_files_list.NextPageToken
    if destPageToken == "" {
      break
    }
  }
 
  pageToken := ""
  count := 0
  //var added_filenames []string
  for {
    // Get list all objects in source bucket.
    source_files := service.Objects.List(*SourceBucketName)
    //source_files.MaxResults(1000)
    source_files.Prefix(*PrefixFileName)
    if pageToken != "" {
      source_files.PageToken(pageToken)
    }
    source_files_list, err := source_files.Context(context.Background()).Do()
    if err != nil {
      fmt.Printf("Objects.List failed: %v\n", err)
      os.Exit(1)
    }
    for _, OneItem := range source_files_list.Items {
      count = count + 1
      // fmt.Printf("Handling source file: %s count: %d\n", OneItem.Name, count)
      if existing_filenames[OneItem.Name] {
        fmt.Printf("object %s already there\n", OneItem.Name)
        continue
      }
      if file_content, err := service.Objects.Get(*SourceBucketName, OneItem.Name).Download(); err == nil {
        // Insert the object into destination bucket.
        object := &storage.Object{Name: OneItem.Name}
        if _, err := service.Objects.Insert(*DestBucketName, object).Media(file_content.Body).Do(); err == nil {
        //fmt.Printf("Created object %v at location %v\n", res.Name, res.SelfLink)
        //added_filenames = append(added_filenames, OneItem.Name)
        //count := count + 1
        //if count > 10 {
        //  break
        //}
        } else {
          fmt.Printf("Objects insert failed: %v\n", err)
          os.Exit(1)
        }
      }
    }
    pageToken = source_files_list.NextPageToken
    if pageToken == "" {
      break
    }
  }
  
  
  // Generate Log for newly added tar files.
  //fmt.Printf("Added files in bucket:\n")
  //for _, object := range added_filenames {
  //  fmt.Println(object)
  //}    
}
