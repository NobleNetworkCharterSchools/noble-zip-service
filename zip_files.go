package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"

    "archive/zip"
    "bytes"
    "fmt"
    "os"
    "strings"
    "sync"
    "time"
)

const (
    bufStartSize int = 1024
    objectBatchSize int64 = 50
)

func exitErrorf(msg string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, msg+"\n", args...)
    os.Exit(1)
}

func main() {
	archive := newSyncedArchive("baseDirName")
	defer archive.archiveFile.Close() // defer in main?

        bucketName := "nncs-zip-test-bucket"
        keyPrefix := "pdfs"

        sess, _ := session.NewSession(&aws.Config{
            Region: aws.String("us-east-1")},
        )

        s3Client := s3.New(sess)
        downloader := s3manager.NewDownloader(sess)

        //s3Client := s3.New(session.New())
        // use s3client.GetBucketLocation to get region from bucket name?

        listInput := &s3.ListObjectsV2Input{
            Bucket: aws.String(bucketName),
            Prefix: aws.String(keyPrefix),
            MaxKeys: aws.Int64(objectBatchSize),
        }

        resultsTruncated := true
        var resultsCount int
        for resultsTruncated {
            resp, err := s3Client.ListObjectsV2(listInput)
            if err != nil {
                exitErrorf("Unable to list items in bucket %q: %v", bucketName, err)
            }

            for _, item := range resp.Contents {
                splitKeys := strings.Split(*item.Key, "/")
                fileName := splitKeys[len(splitKeys)-1]
                archive.wg.Add(1)
                go archive.addToArchive(fileName, bucketName, *item.Key, downloader)
            }

            resultsCount += len(resp.Contents)

            resultsTruncated = *resp.IsTruncated
            //resultsTruncated = false // dev
            listInput.ContinuationToken = resp.NextContinuationToken
            archive.wg.Wait() // limiting concurrent goroutines
            fmt.Println("Done processing batch")
        }

        fmt.Printf("Found %v items in bucket %q\n", resultsCount, bucketName+"/"+keyPrefix)

	err := archive.zipWriter.Close()
        if err != nil {
            exitErrorf("Error saving zip archive: %v", err)
        }
}

type syncedArchive struct {
	dirName     string
	archiveFile *os.File
	zipWriter   *zip.Writer
        wg          *sync.WaitGroup
	*sync.Mutex
}

func newSyncedArchive(dirName string) *syncedArchive {
	// create zip file on disk and a writer to it
	archiveName := fmt.Sprintf("%v.zip", "job_id")
	zipFile, err := os.Create(archiveName)
        if err != nil {
            exitErrorf("Error creating zip file on disk: %v", err)
        }

	zipWriter := zip.NewWriter(zipFile)
	return &syncedArchive{
            dirName,
            zipFile,
            zipWriter,
            &sync.WaitGroup{},
            &sync.Mutex{},
        }
}

func (a *syncedArchive) addToArchive(fileName, bucket, key string, downloader *s3manager.Downloader) {
	defer a.wg.Done()

	// set up header
	var header zip.FileHeader
	header.Method = zip.Deflate // for better compression
	header.Name = fmt.Sprintf("%v/%v", a.dirName, fileName)
	header.Modified = time.Now()

        downloadBuf := aws.NewWriteAtBuffer(make([]byte, bufStartSize))
        numBytes, err := downloader.Download(downloadBuf,
            &s3.GetObjectInput{
                Bucket: aws.String(bucket),
                Key: aws.String(key),
            },
        )
        if err != nil {
            exitErrorf("Unable to download item %q: %v", key, err)
        }
        fmt.Println("Downloaded", key, numBytes, "bytes")

        data := bytes.NewBuffer(downloadBuf.Bytes())

	a.Lock()
	defer a.Unlock()

	// add file to zip archive from buffer
	fileWriter, err := a.zipWriter.CreateHeader(&header)
        if err != nil {
            exitErrorf("Error creating zip writer handle: %v", err)
        }
	numBytes, err = data.WriteTo(fileWriter)
        if err != nil {
            exitErrorf("Error saving data to zip archive: %v", err)
        }
        fmt.Printf("\tWrote %v bytes to zip archive as %q\n", numBytes, fileName)
}

