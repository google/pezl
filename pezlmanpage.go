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
	"fmt"
)

const man string = `pezl(1) - man page

pezl - split an Google Cloud Storage object into smaller internet objects
Synopsis
pezl [OPTION]... INPUT [PREFIX]

Description
Creates kinda fixed-size pieces (pezlings) of INPUT object (verylargefile) to
PEZLING_aa, PEZLING_ab, ... output objects; default size is 1M, and default
PREFIX is the very large file input object name.

-a, --suffix-length=N
     use suffixes of length N (default 2 which provides 676 output files)
-b, --bytes=SIZE
     put exactly SIZE bytes per output file, default is 1M
-l  --lines
     put approximately SIZE bytes of LINES per output file (ends pezlings on
			 the first EOL after nSIZE bytes
-d, --numeric-suffixes
     use numeric suffixes instead of alphabetic (000,001, ... vs aaa,aab,)

-s, --specify
     specify a single pezling to be created. pezl -s 1 would only create
		 a single pezling from byte 1 to byte 1M.  Used to parallelize.
-u, --unpezl
     applies the reverse pezl logic to combine pezlings in lexicographical
		 order into a verylargefile
-v --verbose
     prints diagnostics while pezling
-h --help
     display this help and exit

pezl gs://bucket/verylargefile.txt
creates size/1M pezlings called gs://bucket/verylargefile.txt_aa
gs://bucket/verylargefile.txt_ab gs://bucket/verylargefile.txt_ac...

pezl -b 50000 -l gs://bucket/verylargefile.txt
creates ~50KB  pezlings called gs://bucket/verylargefile.txt_aa that ends with
the first EOL after each 50K step.....

pezl --numeric-suffixes -b 50000 -l -s 3 gs://bucket/verylargefile.txt
creates a single ~50KB  pezling called gs://bucket/verylargefile.txt_03 that
begins after the first EOL after byte 150K and ends with the first EOL after
200KB

Authors
Written by clowndaddy@google.com and baldtim@google.com
`

func displayHelp() {
	fmt.Printf("%s", man)
}
