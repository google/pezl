// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
)

func fmtobjname(n string, s string) string {
	return n + "_" + s
}

func Foo(bkt string, obj string, largefilesize int64, sbkt string, sobj string, ssuffix string, pezlingnum int64, pezlingsize int64, dolines bool) {
	const maxlinelength = 5000 //maximum # chars between newlines

	ctx := context.Background()
	clientr, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("ERR getting storage client context: %v", err)
	}

	totalpezlings := largefilesize / pezlingsize
	modpezlings := largefilesize % *numbytesPtr
	if modpezlings > 0 {
		totalpezlings++
	}
	start := (pezlingnum - 1) * pezlingsize //start of range read
	readsize := pezlingsize
	afterfirstEOL := 0 //default to 0 for first line

	if dolines { // add line pad at end of buffer by expanding readsize
		readsize = readsize + maxlinelength
	}
	if pezlingnum == totalpezlings { //last pezling, less to read
		readsize = modpezlings
	}
	firstEOLafterpezlingsize := readsize //will be updated if we are doing lines

	rc, err := clientr.Bucket(bkt).Object(obj).NewRangeReader(ctx, int64(start), readsize)
	if err != nil {
		fmt.Printf("ERR reading the range: %v\n", err)
	}
	clientr.Close()

	slurp := make([]byte, readsize)
	if _, err = io.ReadFull(rc, slurp); err != nil {
		// we will get unexexpected EOF here with small files
	}
	rc.Close()
	if *verbosePtr {
		fmt.Printf("Snorfle %d read buffer from biglargefile bytes %d to %d\n", pezlingnum, start, (start + readsize))
	}

	// update start and firstEOLafterpezlingsize positions if counting by lines
	if dolines {
		if pezlingnum > 1 { //update start for all but snorlfling #1
			for i := 0; i < maxlinelength; i++ {
				if slurp[i] == 10 {
					afterfirstEOL = i + 1
					break
				}
			}
		}

		if pezlingnum < totalpezlings { //not the last pezling}
			for i := pezlingsize; i < pezlingsize+maxlinelength; i++ {
				if slurp[i] == 10 {
					firstEOLafterpezlingsize = i + 1
					break
				}
			}
		}
	}

	//write the pezling object
	clientw, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("ERR getting storage client context: %v", err)
	}
	pezlingbkt := clientw.Bucket(sbkt)
	pezlingobj := pezlingbkt.Object(fmtobjname(sobj, ssuffix))
	w := pezlingobj.NewWriter(ctx)

	// Write slurp to obj. This will either create the object or overwrite whatever is there already.
	if pezlingnum < totalpezlings { //not the last
		if _, err := w.Write(slurp[afterfirstEOL:firstEOLafterpezlingsize]); err != nil {
			fmt.Printf("ERR writing slurp to pezling object: %v", err)
		}
	} else {
		if _, err := w.Write(slurp[afterfirstEOL:firstEOLafterpezlingsize]); err != nil {
			fmt.Printf("ERR writing slurp to pezling object: %v", err)
		}
	}
	if *verbosePtr {
		fmt.Printf("Pezl %d wrote pezling from buffer bytes %d to %d\n", pezlingnum, afterfirstEOL, firstEOLafterpezlingsize)
	}

	if err := w.Close(); err != nil {
		fmt.Printf("ERR closing the object writer: %v", err)
	}
	clientw.Close()
	goroutinesrunning--
}
