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

// Package that do basic operations given bucket names and file name/prefix,
// such as ls, cp, rm, etc.

package gcs

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
)

var service = CreateService()

// Create singleton GCS service used by all functions.
func CreateService() *storage.Service {
        // This scope allows the application full control over resources in Google Cloud Storage
        var scope = storage.DevstorageFullControlScope
	client, err := google.DefaultClient(context.Background(), scope)
	if err != nil {
		fmt.Printf("Unable to get default client: %v \n", err)
		return nil
	}
	service, err := storage.New(client)
	if err != nil {
		fmt.Printf("Unable to create storage service: %v\n", err)
		return nil
	}
	return service
}

// Create a new Bucket. Return true if already exsit or created successfully.
func CreateBucket(ProjectID string, BucketName string) bool {
	if service == nil {
		fmt.Printf("Cannot create service.\n")
		return false
	}

	if _, err := service.Buckets.Get(BucketName).Do(); err == nil {
		fmt.Printf("Bucket %s already exists.\n", BucketName)
		return true
	} else {
		// Create a bucket.
		if res, err := service.Buckets.Insert(ProjectID, &storage.Bucket{Name: BucketName}).Do(); err == nil {
			fmt.Printf("Created bucket %v at location %v\n", res.Name, res.SelfLink)
		} else {
			fmt.Printf("Failed creating bucket %s: %v\n", BucketName, err)
		}
	}
	return true
}

// Given the bucket name, return array of file names in that bucket. ("ls")
func GetFileNamesFromBucket(BucketName string) []string {
	service := CreateService()
	if service == nil {
		fmt.Printf("Cannot create service.\n")
		return nil
	}

	var FileNames []string
	pageToken := ""
	for {
		call := service.Objects.List(BucketName)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		res, err := call.Do()
		if err != nil {
			fmt.Printf("Get file list failed: %v\n", err)
			return nil
		}
		for _, object := range res.Items {
			FileNames = append(FileNames, object.Name)
		}
		if pageToken = res.NextPageToken; pageToken == "" {
			break
		}
	}
	return FileNames
}

// Delete all files with specified prefix from bucket. If prefix is empty string,
// delete the bucket as well. ("rm")
func DeleteFiles(BucketName string, PrefixFileName string) bool {
	if service == nil {
		fmt.Printf("Cannot create service.\n")
		return false
	}

	_, err := service.Buckets.Get(BucketName).Do()
	if err != nil {
		fmt.Printf("Bucket %s does not exists.\n", BucketName)
		return false
	}

	// Delete files.
	pageToken := ""
	for {
		// Get list all objects in source bucket.
		source_files := service.Objects.List(BucketName)
		source_files.Prefix(PrefixFileName)
		if pageToken != "" {
			source_files.PageToken(pageToken)
		}
		source_files_list, err := source_files.Context(context.Background()).Do()
		if err != nil {
			fmt.Printf("Objects List of source bucket failed: %v\n", err)
			return false
		}
		for _, OneItem := range source_files_list.Items {
			result := service.Objects.Delete(BucketName, OneItem.Name).Do()
			if result != nil {
				fmt.Printf("Objects deletion failed: %v\n", err)
				return false
			}
		}

		if pageToken = source_files_list.NextPageToken; pageToken == "" {
			break
		}
	}

	// Delete the bucket if it is empty.
	if PrefixFileName == "" {
		if err := service.Buckets.Delete(BucketName).Do(); err != nil {
			fmt.Printf("Could not delete bucket %v\n", err)
			return false
		}
	}
	return true
}

// Upload one file from local path to bucket. ("cp")
func UploadFile(BucketName string, FileName string) bool {
	if service == nil {
		fmt.Printf("Cannot create service.\n")
		return false
	}

	file, err := os.Open(FileName)
	if err != nil {
		fmt.Printf("Error opening local file %s: %v\n", FileName, err)
		return false
	}
	object := &storage.Object{Name: filepath.Base(FileName)}
	if res, err := service.Objects.Insert(BucketName, object).Media(file).Do(); err == nil {
		fmt.Printf("Created object %v at location %v\n", res.Name, res.SelfLink)
		return true
	}
	fmt.Printf("Objects.Insert failed: %v\n", err)
	return false
}

// Copy one file from one bucket to another bucket. Return true if succeed. ("cp")
func CopyOneFile(SourceBucket string, DestBucket string, FileName string) bool {
	if service == nil {
		fmt.Printf("Cannot create service.\n")
		return false
	}

	if file_content, err := service.Objects.Get(SourceBucket, FileName).Download(); err == nil {
		object := &storage.Object{Name: FileName}
		_, err := service.Objects.Insert(DestBucket, object).Media(file_content.Body).Do()
		if err != nil {
			fmt.Printf("Objects insert failed: %v\n", err)
			return false
		}
	}
	return true
}

// Copy all files with PrefixFileName from SourceBucke to DestBucket if there
// is no one yet. Return true if succeed.
func SyncTwoBuckets(SourceBucket string, DestBucket string, PrefixFileName string) bool {
	if service == nil {
		fmt.Printf("Cannot create service.\n")
		return false
	}

	// Build list of exisitng files in destination bucket.
	existing_filenames := make(map[string]bool)
	destPageToken := ""
	for {
		destination_files := service.Objects.List(DestBucket)
		if destPageToken != "" {
			destination_files.PageToken(destPageToken)
		}
		destination_files.Prefix(PrefixFileName)
		destination_files_list, err := destination_files.Context(context.Background()).Do()
		if err != nil {
			fmt.Printf("Objects.List failed: %v\n", err)
			return false
		}
		for _, OneItem := range destination_files_list.Items {
			existing_filenames[OneItem.Name] = true
		}
		destPageToken = destination_files_list.NextPageToken
		if destPageToken == "" {
			break
		}
	}

	// Copy files.
	pageToken := ""
	for {
		// Get list all objects in source bucket.
		source_files := service.Objects.List(SourceBucket)
		source_files.Prefix(PrefixFileName)
		if pageToken != "" {
			source_files.PageToken(pageToken)
		}
		source_files_list, err := source_files.Context(context.Background()).Do()
		if err != nil {
			fmt.Printf("Objects List of source bucket failed: %v\n", err)
			return false
		}
		for _, OneItem := range source_files_list.Items {
			if existing_filenames[OneItem.Name] {
				fmt.Printf("object %s already there\n", OneItem.Name)
				continue
			}
			if file_content, err := service.Objects.Get(SourceBucket, OneItem.Name).Download(); err == nil {
				// Insert the object into destination bucket.
				object := &storage.Object{Name: OneItem.Name}
				_, err := service.Objects.Insert(DestBucket, object).Media(file_content.Body).Do()
				if err != nil {
					fmt.Printf("Objects insert failed: %v\n", err)
					return false
				}
			}
		}
		pageToken = source_files_list.NextPageToken
		if pageToken == "" {
			break
		}
	}

	return true
}
