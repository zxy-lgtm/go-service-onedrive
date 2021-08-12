package onedrive

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	. "github.com/beyondstorage/go-storage/v4/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		o = s.newObject(true)
		o.Mode = ModeDir
	} else {
		o = s.newObject(false)
		o.Mode = ModeRead
	}

	o.ID = filepath.Join(s.workDir, path)
	o.Path = path
	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getPath(path)

	_, err = Delete(ctx, s.client, rp)

	if err != nil && errors.Is(err, os.ErrNotExist) {
		// Omit `file not exist` error here
		// ref: [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
		err = nil
	}
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	return
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	var rc io.ReadCloser

	rp := s.getAbsPath(path)
	rc, err = Download(ctx, s.client, rp)
	if err != nil {
		return 0, err
	}

	if err != nil {
		return 0, err
	}

	if opt.HasSize {
		rc = iowrap.LimitReadCloser(rc, opt.Size)
	}
	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(rc, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	fp := s.getPath(path)

	r, err := getSomeObjectPart(ctx, s.client, fp)

	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.Path = path
	o.ID = r.ID

	if r.File == nil && r.Folder == nil {
		return o, errors.New("it is not a file nor a folder")
	}

	if r.File != nil {
		s.formatFileObject(o, r)
	}

	if r.Folder != nil {
		s.formatFolderObject(o, r)
	}

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {

	rp := s.getPath(path)

	r = io.LimitReader(r, size)

	_, err = Upload(ctx, rp, s.client, size, r)
	//f, needClose, err := s.createFile(rp)

	if err != nil {
		return
	}

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	return size, nil
}
