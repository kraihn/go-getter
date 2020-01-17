package getter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	//
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// TODO: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-go?tabs=windows#understand-the-sample-code

// AzureBlobGetter is a Getter implementation that will download a module from
// an Azure Blob Storage Account.
type AzureBlobGetter struct {
	getter
}

func (g *AzureBlobGetter) ClientMode(u *url.URL) (ClientMode, error) {
	// Parse URL
	accountName, baseURL, containerName, blobPath, accessKey, err := g.parseUrl(u)
	if err != nil {
		return 0, err
	}

	client, err := g.getBobClient(accountName, baseURL, accessKey)
	if err != nil {
		return 0, err
	}

	container := client.NewContainerURL(containerName)

	ctx := context.Background()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, _ := container.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: blobPath})
		marker = listBlob.NextMarker

		for _, blobInfo := range listBlob.Segment.BlobItems {
			if blobInfo.Name == blobPath {
				return ClientModeFile, nil
			}

			if strings.HasPrefix(blobInfo.Name, blobPath+"/") {
				return ClientModeDir, nil
			}

			return 0, nil
		}
	}

	return ClientModeFile, nil
}

func (g *AzureBlobGetter) Get(dst string, u *url.URL) error {
	//Parse URL
	accountName, baseURL, containerName, blobPath, accessKey, err := g.parseUrl(u)
	if err != nil {
		return err
	}

	// Remove destination if it already exists
	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if err == nil {
		// Remove the destination
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
	}

	// Create all the parent directories
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	client, err := g.getBobClient(accountName, baseURL, accessKey)
	if err != nil {
		return err
	}

	containerURL := client.NewContainerURL(containerName)

	ctx := context.Background()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, _ := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: blobPath})
		//handleErrors(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			objPath := blobInfo.Name

			// If the key ends with a backslash assume it is a directory and ignore
			if strings.HasSuffix(objPath, "/") {
				continue
			}

			// Get the object destination path
			objDst, err := filepath.Rel(blobPath, objPath)
			if err != nil {
				return err
			}

			objDst = filepath.Join(dst, objDst)

			if err := g.getObject(client, objDst, containerName, objPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *AzureBlobGetter) GetFile(dst string, u *url.URL) error {
	accountName, baseURL, containerName, blobPath, accessKey, err := g.parseUrl(u)
	if err != nil {
		return err
	}

	client, err := g.getBobClient(accountName, baseURL, accessKey)
	if err != nil {
		return err
	}

	return g.getObject(client, dst, containerName, blobPath)
}

func (g *AzureBlobGetter) getObject(serviceURL azblob.ServiceURL, dst, container, blobName string) error {
	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(blobName)

	get, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Fatal(err)
	}

	downloadedData := &bytes.Buffer{}
	reader := get.Body(azblob.RetryReaderOptions{})
	downloadedData.ReadFrom(reader)
	reader.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, downloadedData)
	return err
}

func (g *AzureBlobGetter) getBobClient(accountName string, baseURL string, accountKey string) (azblob.ServiceURL, error) {
	// Use your Storage account's name and key to create a credential object; this is used to access your account.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.%s", accountName, baseURL))

	serviceURL := azblob.NewServiceURL(*u, p)

	return serviceURL, nil
}

func (g *AzureBlobGetter) parseUrl(u *url.URL) (accountName, baseURL, container, blobPath, accessKey string, err error) {
	// Expected host style: accountname.blob.core.windows.net.
	// The last 3 parts will be different across environments.
	hostParts := strings.SplitN(u.Host, ".", 3)
	if len(hostParts) != 3 {
		err = fmt.Errorf("URL is not a valid Azure Blob URL")
		return
	}

	accountName = hostParts[0]
	baseURL = hostParts[2]

	pathParts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(pathParts) != 2 {
		err = fmt.Errorf("URL is not a valid Azure Blob URL")
		return
	}

	container = pathParts[0]
	blobPath = pathParts[1]

	accessKey = u.Query().Get("access_key")

	return
}
