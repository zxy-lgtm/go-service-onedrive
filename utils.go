package onedrive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	typ "github.com/beyondstorage/go-storage/v4/types"
	"github.com/goh-chunlin/go-onedrive/onedrive"
	"golang.org/x/oauth2"
)

const token = "EwCIA8l6BAAU6k7+XVQzkGyMv7VHB/h4cHbJYRAAAZs+FvIKuFfahXNDEbwQUG1HM+sDhXsfnlSd4p3tkd91ayqwwin4ZYmIe4KFEnNtElbBkuUKThQ4snU962jy194OygTBRxi5zaxDFwVayhqAWRd4DPPOtkvG4hy2kyx91b92T6jI5wEP+wpQCppaGpKrBJ1AaiebXgc7M9b8AhNe1OAMziTx3xb8BFZLwqzqGCj7xGxD4oqWmZZweT7v5wu+ROcxNJ1Ujm5OC8YNKymciG/d02kGqM6bx+Yp3W2APDMC6GAwTFni6QLc0/mH0iY0onW+7sfRza8h1/j8BpFXpVfudRTg10VH/jI00FHqruyT8EAxCLFOg7iZ9JNJYoEDZgAACIIvh4+6P3L1WAKJiuGISK2axgwro4G2A0ELQDV/D2/lAFPx6jisSfyFcuC0nivorHuHNxFFzOww8UavovQ8qgNbbDv1mMH9GSF+DiyPPRZGqz6aXT5wyeVV7j3fkBWAyHB7TivQ1cpZIP+T09oaoFuP4mpbO/KA8iBgZWOols5I5B2/QMUK7INefLyoc9xfH+gFu9vjRXWp4iSsPLHkRcKfupnsY3xvf8ej7ARMR7dMlhFBD1cNp0BTKoy/EC1T3maS2Kmg/6HSyWQ31v4z0ZdIoFT7iY1DMxC4evWI1veMUR8QYYCzxuT1l2t1rQx38KL59bA2JArislq8M3PUvADVi98C8VGaFZGg06M0TpETRZnp5vXfeS3HNllamQd0PXsYk7QMpxPRm4pkTFe8Vss4t1c4mqb/8ARhWoikAdPqJojKwRv06hgF25/9eZO2tg/AbYgmcm2w5DnW49k3DdpeFVbUQYCtrw7JPcpKnZW/jxZddgaxdnN8BRY6D2Z/y75re3UTCYXSbjRsYofcCSQg89wT3nROAesjBW/ouZG7QV2jXjI027rPwAUk+ao3HW+ykEzE5uKBCQJraEOl866t/yUEpU8Eu0rl66sUosxr1+OCADli/6vYHHj5X0pnR4UVmE0Tb7Iv6eBMUyy0Cm4lznVI50ucPUyMKq9iRqXLZE7QJfTqMB6ne1CpExFSb10/gVsg4uvsGGXkjUkbqYgKAjQbTiHDNIVE2xhRpqpQ4n/91W5Jw9rllOUq+H5A3h9Y85hkzPxbkPRpJ49c1uKOUYpSPbdtBe/2LCs5I8/TkpmNAg=="

const (
	// Std{in/out/err} support
	Stdin  = "/dev/stdin"
	Stdout = "/dev/stdout"
	Stderr = "/dev/stderr"

	PathSeparator = string(filepath.Separator)
)

// Storage is the onedrive client.
type Storage struct {
	client       *onedrive.Client
	workDir      string
	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	typ.UnimplementedStorager
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf("Storager onedrive {WorkDir: %s}", s.workDir)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...types.Pair) (types.Storager, error) {
	return newStorager(pairs...)
}

func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	// Handle error returned by os package.
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
	case errors.Is(err, os.ErrPermission):
		return fmt.Errorf("%w: %v", services.ErrPermissionDenied, err)
	default:
		return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
	}
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

// newStorager will create a onedrive client.
func newStorager(pairs ...typ.Pair) (store *Storage, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		}
	}()
	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return
	}

	commonCtx := context.Background()

	store = &Storage{
		client:  getClient(commonCtx),
		workDir: "/",
	}

	if opt.HasDefaultStoragePairs {
		store.defaultPairs = opt.DefaultStoragePairs
	}
	if opt.HasStorageFeatures {
		store.features = opt.StorageFeatures
	}
	if opt.HasWorkDir {
		store.workDir = opt.WorkDir
	}

	// Check and create work dir
	err = os.MkdirAll(store.workDir, 0755)
	if err != nil {
		return nil, err
	}
	return
}

func (s *Storage) newObject(done bool) *typ.Object {
	return typ.NewObject(s, done)
}

func (s *Storage) getPath(path string) string {
	//去掉path末尾可能存在的“/”
	path = strings.TrimRight(path, "/")

	if filepath.IsAbs(path) {
		return path
	}
	absPath := s.workDir + "/" + path
	return absPath

}

func (s *Storage) openFile(absPath string, mode int) (f *os.File, needClose bool, err error) {
	switch absPath {
	case Stdin:
		f = os.Stdin
	case Stdout:
		f = os.Stdout
	case Stderr:
		f = os.Stderr
	default:
		needClose = true
		f, err = os.OpenFile(absPath, mode, 0664)
	}

	return
}

func (s *Storage) createFile(absPath string) (f *os.File, needClose bool, err error) {
	switch absPath {
	case Stdin:
		return os.Stdin, false, nil
	case Stdout:
		return os.Stdout, false, nil
	case Stderr:
		return os.Stderr, false, nil
	}

	fi, err := os.Lstat(absPath)
	if err == nil {
		// File is exist, let's check if the file is a dir or a symlink.
		if fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, false, services.ErrObjectModeInvalid
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		// Something error other than ErrNotExist happened, return directly.
		return
	}
	// Set stat error to nil.
	err = nil

	// The file is not exist, we should create the dir and create the file.
	if fi == nil {
		err = os.MkdirAll(filepath.Dir(absPath), 0755)
		if err != nil {
			return nil, false, err
		}
	}

	// There are two situations we handled here:
	// - The file is exist and not a dir
	// - The file is not exist
	// It's OK to open them with O_CREATE|O_TRUNC.
	f, err = os.Create(absPath)
	if err != nil {
		return nil, false, err
	}
	return f, true, nil
}

func (s *Storage) formatFolderObject(o *typ.Object, r *Response) *typ.Object {
	o.Mode |= typ.ModeDir
	o.Mode |= typ.ModeRead

	o.SetContentLength(r.Size)
	o.SetLastModified(r.LastModifiedDateTime)
	o.SetEtag(r.Etag)

	return o
}

func (s *Storage) formatFileObject(o *typ.Object, r *Response) *typ.Object {
	o.Mode |= typ.ModeRead

	o.SetContentLength(r.Size)
	o.SetLastModified(r.LastModifiedDateTime)
	o.SetEtag(r.Etag)

	return o
}

func (s *Storage) getAbsPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	absPath := filepath.Join(s.workDir, path)

	// Join will clean the trailing "/", we need to append it back.
	if strings.HasSuffix(path, PathSeparator) {
		absPath += PathSeparator
	}
	return absPath
}

// path 一定要是绝对路径，并且不以 / 结尾，就算是文件夹也是，要处理一下
func getItem(ctx context.Context, client *onedrive.Client, path string) (*onedrive.DriveItem, bool) {
	searchItem := func(folderId string) []*onedrive.DriveItem {
		listRes, err := client.DriveItems.List(ctx, folderId)
		if err != nil {
			panic(err)
		}

		return listRes.DriveItems
	}

	paths := strings.Split(path, "/")
	lenp := len(paths)

	// 从 root 也就是 "" 开始
	nextId := ""
	var searchRes *onedrive.DriveItem
	for i := 1; i < lenp; i++ {
		// 当前搜索的目标名字
		nowSearchValue := paths[i]
		list := searchItem(nextId)
		for _, v := range list {
			if v.Name == nowSearchValue {
				nextId = v.Id
				// 最后一个，也就是目标文件名了
				if i == lenp-1 {
					searchRes = v
				}
				break
			}
		}
	}

	if searchRes == nil {
		return nil, false
	}

	return searchRes, true
}

func getClient(ctx context.Context) *onedrive.Client {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)

	tc := oauth2.NewClient(ctx, ts)

	client := onedrive.NewClient(tc)

	return client
}

func getItemId(ctx context.Context, client *onedrive.Client, path string) string {
	item, ok := getItem(ctx, client, path)

	if !ok {
		return ""
	}

	return item.Id
}
