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
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// parse the flags and passed parameters using flag module
var suffixlengthPtr = flag.Int64("a", 2, "use suffixes of length N (default 2 which provides 676 output files)")
var numbytesPtr = flag.Int64("b", 1024*1024, "put exactly SIZE bytes per output file, default is 1M")
var dolinesPtr = flag.Bool("l", false, "put approximately SIZE bytes of lines per output file (ends pezlings on the first EOL after nSIZE bytes")
var numsuffixPtr = flag.Bool("d", false, "use numeric suffixes instead of alphabetic")
var specifyPtr = flag.Int64("s", 0, "specify a single pezling to be created. pezl -s 1 would only create a single pezling from byte 1 to byte 1M.  This is often called in a distributed fashion.")
var numthreadsPtr = flag.Int64("t", 100, "Specify the number of concurrent goroutines ('threads') to use.")
var unsnorfPtr = flag.Bool("u", false, "Unpezl (concatenate) files created by pezl")
var verbosePtr = flag.Bool("v", false, "prints diagnostics while pezling")
var helpPtr = flag.Bool("h", false, "display this help and exit")

var verylargefile, verylargefilebucket, verylargefilekey, pezlingprefix, pezlingbucket, pezlingkey string
var largefilesize, numpezlings, goroutinesrunning int64

func displayFlags() {
	fmt.Println("a:", *suffixlengthPtr)
	fmt.Println("b:", *numbytesPtr)
	fmt.Println("l:", *dolinesPtr)
	fmt.Println("d:", *numsuffixPtr)
	fmt.Println("s:", *specifyPtr)
	fmt.Println("t:", *numthreadsPtr)
	fmt.Println("u:", *unsnorfPtr)
	fmt.Println("v:", *verbosePtr)
	fmt.Println("h:", *helpPtr)
	fmt.Println("verylargefile: ", verylargefile)
	fmt.Println("pezlingprefix: ", pezlingprefix)
}

// calculates a lexicographical pezling object name suffix string for
// alpha or numeric naming when given an object number
func pezlingsuffix(pezling int64, suffixlength int64, numeric bool) string {
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	suffix := ""
	pezling-- //we count from 1 elsewhere, from 0 here
	remainder := pezling
	if numeric {
		suffix = fmt.Sprintf("%0*d", suffixlength, pezling)
	} else {
		for i := (suffixlength - 1); i > -1; i-- { // most significant digit first
			suffix = suffix + string(alphabet[remainder/timexp(26, i)])
			remainder = remainder % timexp(26, i)
		}
	}
	return (suffix)
}

// TIL that golang doesn't have an exponent operator.  Quick hack for A^B ints
func timexp(base int64, exp int64) int64 {
	val := int64(1)
	for i := int64(0); i < exp; i++ {
		val = base * val
	}
	return (val)
}

// mainloop interprets flags, and is single goroutine except for pezl a verylargefile
// into multiple pezlings.  Concurrency is limited by "threads" flag to avoid
// misbehavior if we run out of memory/ports/etc.
func main() {
	// complete parsing flags and set prefix if not set
	flag.Parse()
	verylargefile = flag.Arg(0)
	pezlingprefix = flag.Arg(1)
	if len(pezlingprefix) == 0 {
		pezlingprefix = verylargefile
	}

	// if -h is passed, terminate gracefully after displaying help screen
	if *helpPtr {
		displayHelp()
		os.Exit(0)
	}

	// for debugging, dump all flags passed in verbose mode, will also be used
	// regularly to control verbosity
	if *verbosePtr {
		displayFlags()
	}

	// parse the gs:// filepath for verylargefile, hack knowing gs:// format
	s := strings.Split(verylargefile, "/")
	if (s[0] == "gs:") && (len(s) >= 3) {
		verylargefilebucket = s[2]
		verylargefilekey = strings.Join(s[3:], "/")
	} else {
		fmt.Println("1-Input/Prefix must be in GCP gs://bucket/key notation, exiting")
		os.Exit(0)
	}

	s = strings.Split(pezlingprefix, "/")
	if (s[0] == "gs:") && (len(s) >= 3) {
		pezlingbucket = s[2]
		pezlingkey = strings.Join(s[3:], "/")
	} else {
		fmt.Println("Input/Prefix must be in GCP gs://bucket/key notation, exiting")
		os.Exit(0)
	}

	if *unsnorfPtr {
		unpezl(verylargefilebucket, verylargefilekey, pezlingbucket, pezlingkey+"_")
		os.Exit(0)
	}

	// read the input file metadata
	if *verbosePtr {
		fmt.Println("Calling GCS to get the input object details")
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("ERR getting storage client context: %v", err)
	}
	rc, err := client.Bucket(verylargefilebucket).Object(verylargefilekey).NewReader(ctx)
	if err != nil {
		fmt.Printf("ERR getting storage client reader: %v", err)
	}
	defer rc.Close()
	objAttrs, err := client.Bucket(verylargefilebucket).Object(verylargefilekey).Attrs(ctx)
	if err != nil {
		fmt.Printf("ERR getting object attributes: %v", err)
	}

	// calculate the filecounts and last pezling filesize
	largefilesize = objAttrs.Size
	numpezlings = largefilesize / *numbytesPtr
	moduluspezlings := largefilesize % *numbytesPtr
	if moduluspezlings > 0 {
		numpezlings++
	}

	// check to see if suffix address space is big enough - should automate
	if *numsuffixPtr { //
		if timexp(10, *suffixlengthPtr) < numpezlings {
			fmt.Printf("Numeric Suffix Length is insufficient for %d pezlings", numpezlings)
			os.Exit(0)
		}
	} else {
		if timexp(26, *suffixlengthPtr) < numpezlings {
			fmt.Printf("Alpha Suffix Length is insufficient for %d pezlings, %d", numpezlings, timexp(26, *suffixlengthPtr))
			os.Exit(0)
		}
	}

	if *verbosePtr {
		fmt.Printf("verylargefile %s has size %d\nOur pezlings are %d bytes\nWe will need %d total pezlings\nAnd %d bytes in the last pezling\n",
			objAttrs.Name, largefilesize, *numbytesPtr, numpezlings, moduluspezlings)
	}

	//call the goroutines

	if *specifyPtr > 0 { //a single pezling was called
		goroutinesrunning++
		if *specifyPtr > numpezlings { //last pezling gets the modulus as size
			fmt.Printf("ERR, specified pezling-%d is > number of pezlings-%d", *specifyPtr, numpezlings)
		} else { //call a single pezling, do not use goroutine
			if *verbosePtr {
				fmt.Printf("calling single Foo-%d \n", *specifyPtr)
			}
			Foo(verylargefilebucket, verylargefilekey, largefilesize, pezlingbucket, pezlingkey, pezlingsuffix(*specifyPtr, *suffixlengthPtr, *numsuffixPtr), *specifyPtr, *numbytesPtr, *dolinesPtr)
		}
	} else { //we do all the pezlings
		for i := 1; i <= int(numpezlings); i++ {
			if *verbosePtr {
				fmt.Printf("Main calling goFoo-%d \n", i)
			}
			go Foo(verylargefilebucket, verylargefilekey, largefilesize, pezlingbucket, pezlingkey, pezlingsuffix(int64(i), *suffixlengthPtr, *numsuffixPtr), int64(i), *numbytesPtr, *dolinesPtr)
			goroutinesrunning++
			// hack to limit concurrency to "threads" specified.
			for goroutinesrunning >= *numthreadsPtr {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	//now we wait for running goroutines to complete to the main loop (hack)
	for goroutinesrunning > 0 {
		time.Sleep(50 * time.Millisecond)
	}
}
