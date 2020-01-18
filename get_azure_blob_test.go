package getter

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The following storage account must consist of a container named `go-getter` with access type
// blob and contain the following blobs:
//   folder/main.tf
//   folder/subfolder/sub.tf
//   collision/foo
//   collision/foo/bar
const azureBlobURL = "https://gaodnn4xiwdhaf45grxl4e7n.blob.core.windows.net"
const sasToken = "sv=2019-02-02&ss=b&srt=sco&sp=rl&se=2020-01-18T11:54:44Z&st=2020-01" + "-18T03:54:44Z&spr=https&sig=eUCMteyC92gorqCHmg1qdafux%2BxwxpKTpcBjKfoJbAY%3D"

func TestAzureBlob_impl(t *testing.T) {
	var _ Getter = new(AzureBlobGetter)
}

func TestAzureBlobGetter(t *testing.T) {
	g := new(AzureBlobGetter)
	dst := tempDir(t)

	// With a dir that doesn't exist
	err := g.Get(
		dst, testURL(fmt.Sprintf("%s/go-getter/folder?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Verify the main file exists
	mainPath := filepath.Join(dst, "main.tf")
	if _, err := os.Stat(mainPath); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestAzureBlobGetter_subdir(t *testing.T) {
	g := new(AzureBlobGetter)
	dst := tempDir(t)

	// With a dir that doesn't exist
	err := g.Get(
		dst, testURL(fmt.Sprintf("%s/go-getter/folder/subfolder?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Verify the main file exists
	subPath := filepath.Join(dst, "sub.tf")
	if _, err := os.Stat(subPath); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestAzureBlobGetter_GetFile(t *testing.T) {
	g := new(AzureBlobGetter)
	dst := tempTestFile(t)

	// Download
	err := g.GetFile(
		dst, testURL(fmt.Sprintf("%s/go-getter/folder/main.tf?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Verify the main file exists
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("err: %s", err)
	}
	assertContents(t, dst, "# Main")
}

func TestAzureBlobGetter_GetFile_badParams(t *testing.T) {
	g := new(AzureBlobGetter)
	dst := tempTestFile(t)

	// Download
	err := g.GetFile(
		dst,
		testURL(fmt.Sprintf("%s/go-getter/folder/main.tf?access_key=foo", azureBlobURL)))
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func TestAzureBlobGetter_GetFile_notfound(t *testing.T) {
	g := new(AzureBlobGetter)
	dst := tempTestFile(t)

	// Download
	err := g.GetFile(
		dst, testURL(fmt.Sprintf("%s/go-getter/folder/404.tf?%s", azureBlobURL, sasToken)))
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func TestAzureBlobGetter_ClientMode_dir(t *testing.T) {
	g := new(AzureBlobGetter)

	// Check client mode on a key prefix with only a single key.
	mode, err := g.ClientMode(
		testURL(fmt.Sprintf("%s/go-getter/folder?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if mode != ClientModeDir {
		t.Fatal("expect ClientModeDir")
	}
}

func TestAzureBlobGetter_ClientMode_file(t *testing.T) {
	g := new(AzureBlobGetter)

	// Check client mode on a key prefix which contains sub-keys.
	mode, err := g.ClientMode(
		testURL(fmt.Sprintf("%s/go-getter/folder/main.tf?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if mode != ClientModeFile {
		t.Fatal("expect ClientModeFile")
	}
}

func TestAzureBlobGetter_ClientMode_notfound(t *testing.T) {
	g := new(AzureBlobGetter)

	// Check the client mode when a non-existent key is looked up. This does not
	// return an error, but rather should just return the file mode so that Azure
	// can return an appropriate error later on. This also checks that the
	// prefix is handled properly (e.g., "/fold" and "/folder" don't put the
	// client mode into "dir".
	mode, err := g.ClientMode(
		testURL(fmt.Sprintf("%s/go-getter/fold?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	fmt.Println(mode)
	if mode != ClientModeFile {
		t.Fatal("expect ClientModeFile")
	}
}

func TestAzureBlobGetter_ClientMode_collision(t *testing.T) {
	g := new(AzureBlobGetter)

	// Check that the client mode is "file" if there is both an object and a
	// folder with a common prefix (i.e., a "collision" in the namespace).
	mode, err := g.ClientMode(
		testURL(fmt.Sprintf("%s/go-getter/collision/foo?%s", azureBlobURL, sasToken)))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if mode != ClientModeFile {
		t.Fatal("expect ClientModeFile")
	}
}
