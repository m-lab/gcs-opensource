// Embargo was implemented inside gcs package.
package gcs

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/net/context"
	storage "google.golang.org/api/storage/v1"
)

var (
	embargoService = createService()
	embargoDate    = "20160305"
	whitelist      = ReadWhitelistFromGCS("whitelist")
	sourceBucket   = "sidestream-embargo"
	destBucket     = "embargo-output"
)

// ReadWhitelistFromLocal load IP whitelist into a vector of strings.
func ReadWhitelistFromLocal(path string) map[string]bool {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()

	whiteList := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		oneLine := strings.TrimSuffix(scanner.Text(), "\n")
		whiteList[oneLine] = true
	}

	return whiteList
}

// ReadWhitelistFromGCS load IP whitelist from cloud storage.
func ReadWhitelistFromGCS(path string) map[string]bool {
	if embargo_service == nil {
		fmt.Printf("Storage service was not initialized.\n")
		return nil
	}
	whiteList := make(map[string]bool)
	if fileContent, err := embargo_service.Objects.Get(source_bucket, path).Download(); err == nil {
		scanner := bufio.NewScanner(fileContent.Body)
		for scanner.Scan() {
			oneLine := strings.TrimSuffix(scanner.Text(), "\n")
			white_list[oneLine] = true
		}
		return whiteList
	}
	return nil
}

// GetLocalIP parse the filename and return IP. For old format, it will return empty string.
func GetLocalIP(fileName string) string {
	localIPStart := strings.IndexByte(fileName, '_')
	localIPEnd := strings.LastIndexByte(fileName, '_')
	if localIPStart < 0 || localIPEnd < 0 || localIPStart >= localIPEnd {
		return ""
	}
	return fileName[localIPStart+1 : localIPEnd]
}

// EmbargoCheck decide whether to embargo it based on embargo date and IP
// whitelist given a filename of sidestream test.
// The filename is like: 20170225T23:00:00Z_4.34.58.34_0.web100.gz
// THe embargo date is like 20160225
// file with date on or before the embargo date are always published. Return false
// file with IP that is in the IP whitelist are always published. Return false
// file with date after the embargo date and IP not in the whitelist will be embargoed. Return true
func EmbargoCheck(fileName string, whitelist map[string]bool) bool {
	date, err := strconv.Atoi(fileName[0:8])
	if err != nil {
		fmt.Println(err)
		return true
	}
	embargoDateInt, err := strconv.Atoi(embargoDate)
	if err != nil {
		fmt.Println(err)
		return true
	}
	if date < embargoDateInt {
		return false
	}
	localIP := GetLocalIP(fileName)
	// For old filename, that do not contain IP, always embargo them.
	if whitelist[localIP] {
		return false
	}
	return true
}

// EmbargoOneTar process one tar file, split it to 2 files, the embargoed files
// will be saved in a private dir, and the unembargoed part will be save in a
// public dir.
func EmbargoOneTar(content io.Reader, tarfileName string) bool {
	// Create tar reader
	zipReader, err := gzip.NewReader(content)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer zipReader.Close()
	unzippedImage, err := ioutil.ReadAll(zipReader)
	if err != nil {
		fmt.Println(err)
		return false
	}
	unzippedReader := bytes.NewReader(unzippedImage)
	tarReader := tar.NewReader(unzippedReader)

	// Create buffer for output
	var privateBuf bytes.Buffer
	var publicBuf bytes.Buffer
	privateGzw := gzip.NewWriter(&privateBuf)
	publicGzw := gzip.NewWriter(&publicBuf)
	privateTw := tar.NewWriter(privateGzw)
	publicTw := tar.NewWriter(publicGzw)

	// Handle one tar file
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			return false
		}
		fmt.Printf(header.Name + "\n")
		basename := filepath.Base(header.Name)
		info := header.FileInfo()
		hdr := new(tar.Header)
		hdr.Name = header.Name
		hdr.Size = info.Size()
		hdr.Mode = int64(info.Mode())
		hdr.ModTime = info.ModTime()
		output, err := ioutil.ReadAll(tarReader)
		if strings.Contains(basename, "web100") && EmbargoCheck(basename, whitelist) {
			// put this file to a private buffer
			if err := privateTw.WriteHeader(hdr); err != nil {
				fmt.Println(err)
				return false
			}
			if _, err := privateTw.Write([]byte(output)); err != nil {
				fmt.Println(err)
				return false
			}
		} else {
			// put this file to a public buffer
			if err := publicTw.WriteHeader(hdr); err != nil {
				fmt.Println(err)
				return false
			}
			if _, err := publicTw.Write([]byte(output)); err != nil {
				fmt.Println(err)
				return false
			}
		}
	}

	if err := publicTw.Close(); err != nil {
		fmt.Println(err)
		return false
	}
	if err := privateTw.Close(); err != nil {
		fmt.Println(err)
		return false
	}
	if err := publicGzw.Close(); err != nil {
		fmt.Println(err)
		return false
	}
	if err := privateGzw.Close(); err != nil {
		fmt.Println(err)
		return false
	}

	publicObject := &storage.Object{Name: "public/" + tarfileName}
	privateObject := &storage.Object{Name: "private/" + tarfileName}
	if _, err := embargo_service.Objects.Insert(destBucket, publicObject).Media(&publicBuf).Do(); err != nil {
		fmt.Printf("Objects insert failed: %v\n", err)
		return false
	}

	if _, err := embargo_service.Objects.Insert(destBucket, privateObject).Media(&privateBuf).Do(); err != nil {
		fmt.Printf("Objects insert failed: %v\n", err)
		return false
	}
	return true
}

// Embargo do embargo ckecking to all files in the sourceBucket.
func Embargo() bool {
	if embargoService == nil {
		fmt.Printf("Storage service was not initialized.\n")
		return false
	}

	sourceFiles := embargoService.Objects.List(sourceBucket)
	sourceFilesList, err := sourceFiles.Context(context.Background()).Do()
	if err != nil {
		fmt.Printf("Objects List of source bucket failed: %v\n", err)
		return false
	}
	for _, oneItem := range sourceFilesList.Items {
		fmt.Printf(oneItem.Name + "\n")
		if !strings.Contains(oneItem.Name, "tgz") || !strings.Contains(oneItem.Name, "sidestream") {
			continue
		}

		fileContent, err := embargoService.Objects.Get(sourceBucket, oneItem.Name).Download()
		if err != nil {
			fmt.Println(err)
			return false
		}
		if !EmbargoOneTar(fileContent.Body, oneItem.Name) {
			return false
		}
	}
	return true
}
