package getter

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	blobURLParts := azblob.NewBlobURLParts(*u)
	client, err := g.getBobClient(blobURLParts, "")
	if err != nil {
		return 0, err
	}

	container := client.NewContainerURL(blobURLParts.ContainerName)

	ctx := context.Background()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, _ := container.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: blobURLParts.BlobName})

		marker = listBlob.NextMarker

		for _, blobInfo := range listBlob.Segment.BlobItems {
			if blobInfo.Name == blobURLParts.BlobName {
				return ClientModeFile, nil
			}

			if strings.HasPrefix(blobInfo.Name, blobURLParts.BlobName+"/") {
				return ClientModeDir, nil
			}
		}
	}

	return ClientModeFile, nil
}

func (g *AzureBlobGetter) Get(dst string, u *url.URL) error {
	//Parse URL
	blobURLParts := azblob.NewBlobURLParts(*u)

	// Remove destination if it already exists
	_, err := os.Stat(dst)
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

	client, err := g.getBobClient(blobURLParts, "")
	if err != nil {
		return err
	}

	containerURL := client.NewContainerURL(blobURLParts.ContainerName)

	ctx := context.Background()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, _ := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: blobURLParts.BlobName})

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
			objDst, err := filepath.Rel(blobURLParts.BlobName, objPath)
			if err != nil {
				return err
			}

			objDst = filepath.Join(dst, objDst)

			if err := g.getObject(client, objDst, blobURLParts.ContainerName, objPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *AzureBlobGetter) GetFile(dst string, u *url.URL) error {
	blobURLParts := azblob.NewBlobURLParts(*u)
	client, err := g.getBobClient(blobURLParts, "")
	if err != nil {
		return err
	}

	return g.getObject(client, dst, blobURLParts.ContainerName, blobURLParts.BlobName)
}

func (g *AzureBlobGetter) getObject(serviceURL azblob.ServiceURL, dst, container, blobName string) error {
	ctx := context.Background()
	containerURL := serviceURL.NewContainerURL(container)
	blobURL := containerURL.NewBlockBlobURL(blobName)

	get, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return err
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

func (g *AzureBlobGetter) getBobClient(blobUrlParts azblob.BlobURLParts, accountKey string) (azblob.ServiceURL, error) {
	accountName := strings.SplitN(blobUrlParts.Host, ".", 3)[0]

	var credential azblob.Credential
	var err error

	if accountKey != "" {
		credential, err = azblob.NewSharedKeyCredential(accountName, accountKey)
	} else {
		credential = azblob.NewAnonymousCredential()
	}

	if err != nil {
		return azblob.ServiceURL{}, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u := blobUrlParts.URL()

	fqdn, _ := url.Parse(fmt.Sprintf("https://%s?%s", u.Host, u.RawQuery))

	serviceURL := azblob.NewServiceURL(*fqdn, p)

	return serviceURL, nil
}
