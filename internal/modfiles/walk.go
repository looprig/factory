// Package modfiles enumerates Go source files owned by this module while excluding
// structural directories and nested repository or module boundaries.
package modfiles

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// SymlinkError reports a module-owned Go source path that is a symbolic link.
// Discovery fails closed rather than following a link that may escape the module.
type SymlinkError struct {
	Path string
}

func (e *SymlinkError) Error() string {
	return "modfiles: module-owned Go file is a symbolic link: " + e.Path
}

// Files returns absolute paths to every module-owned Go source file below root.
// Build constraints are deliberately ignored so tagged production files remain
// visible to dependency and formatting checks.
func Files(root string) ([]string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	var files []string
	err = filepath.WalkDir(absoluteRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != absoluteRoot && entry.IsDir() {
			if _, ignored := IgnoredDirectory(entry.Name()); ignored {
				return filepath.SkipDir
			}
			nested, err := nestedBoundary(path)
			if err != nil {
				return err
			}
			if nested {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if _, ignored := IgnoredFile(entry.Name()); ignored {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return &SymlinkError{Path: path}
		}
		if info.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	return files, nil
}

// WriteNull writes paths separated and terminated by NUL bytes for safe piping to
// tools such as xargs -0, including when a filename contains whitespace or newlines.
func WriteNull(w io.Writer, paths []string) error {
	for _, path := range paths {
		if _, err := io.WriteString(w, path); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\x00"); err != nil {
			return err
		}
	}
	return nil
}

// The reasons the walk refuses a path. They are exported because a guard that
// polices this walk must be able to report WHICH rule hid a file, and must be
// unable to fall behind when a rule is added.
const (
	// ReasonVendor is a vendored dependency tree. No repository in this
	// workspace vendors; Go also ignores vendor/ when a workspace is active.
	ReasonVendor = "vendor"
	// ReasonTestdata is Go's testdata convention: never part of a build.
	ReasonTestdata = "testdata"
	// ReasonDotPrefixed and ReasonUnderscorePrefixed are the names the Go tool
	// itself ignores, which is why they are sound to skip here.
	ReasonDotPrefixed        = "dot-prefixed"
	ReasonUnderscorePrefixed = "underscore-prefixed"
)

// boundaryMarkers are the files whose presence makes a directory a separate
// module or repository, and therefore not this module's content.
var boundaryMarkers = []string{".git", "go.mod"}

// BoundaryMarkers returns the marker filenames that stop the walk at a nested
// module or repository. The slice is a copy: a caller pinning this set must not
// be able to change it.
//
// It is exported so a guard can enumerate the markers rather than restate them.
// A restated copy is a second implementation of "what is a boundary", and two
// such implementations have already disagreed here: one recognised only a .git
// DIRECTORY while this walk stops at a .git FILE too, which is what `git
// submodule add` and `git worktree add` write.
func BoundaryMarkers() []string {
	return slices.Clone(boundaryMarkers)
}

// IgnoredDirectory reports whether the walk refuses to descend into a directory
// with this name, and why.
//
// The reason is returned, not just the verdict, because a guard reporting a
// hidden subtree must say which rule hid it — and because a rule added here
// without a reason cannot be reported at all, which is the failure this
// signature exists to make impossible.
func IgnoredDirectory(name string) (string, bool) {
	switch name {
	case "vendor":
		return ReasonVendor, true
	case "testdata":
		return ReasonTestdata, true
	}
	return goIgnoredName(name)
}

// IgnoredFile reports whether the walk refuses to read a file with this name,
// and why. Go ignores these names too, so a Go file behind one is not part of
// any build.
func IgnoredFile(name string) (string, bool) {
	return goIgnoredName(name)
}

func goIgnoredName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	switch name[0] {
	case '.':
		return ReasonDotPrefixed, true
	case '_':
		return ReasonUnderscorePrefixed, true
	}
	return "", false
}

func nestedBoundary(path string) (bool, error) {
	for _, marker := range boundaryMarkers {
		_, err := os.Lstat(filepath.Join(path, marker))
		switch {
		case err == nil:
			return true, nil
		case os.IsNotExist(err):
			continue
		default:
			return false, err
		}
	}
	return false, nil
}
