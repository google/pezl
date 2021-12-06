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
	"google.golang.org/api/iterator"
	"time"
)

func unpezl(fbkt string, fobj string, sbkt string, sobj string) {
	//create slice of objects to compose
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("ERR getting storage client context: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*360)
	defer cancel()

	// get writer ready for composing biglargefile while unsnorfing
	largefilebkt := client.Bucket(fbkt)
	largefileobj := largefilebkt.Object(fobj)

	//get an iterator of all snorlfings
	it := client.Bucket(sbkt).Objects(ctx, &storage.Query{
		Prefix:    sobj,
		Delimiter: "/",
	})

	p := iterator.NewPager(it, 30, "")
	// since we can only compose 32 objects at a time, we will paginate 30
	// note listing objects uses an iterator, but sadly 'compose' doesn't
	// note the slices hack with handles below
	for i := 0; ; i++ {
		var objectattrz []*storage.ObjectAttrs
		var objecthandlz []*storage.ObjectHandle
		if i > 0 {
			objecthandlz = append(objecthandlz, largefileobj)
		}
		nextPageToken, err := p.NextPage(&objectattrz)
		if err != nil {
			fmt.Printf("ERR getting page token: %v", err)
			break
		}

		for _, b := range objectattrz {
			if *verbosePtr {
				fmt.Printf("unpezl composing pezling-%s, size-%d, %s \n", b.Name, b.Size, nextPageToken)
			}
			objecthandlz = append(objecthandlz, client.Bucket(sbkt).Object(b.Name))
		}

		// Just compose.
		_, err = largefileobj.ComposerFrom(objecthandlz...).Run(ctx)
		if err != nil {
			fmt.Printf("ERR composing: %v", err)
			break
		}
		if nextPageToken == "" {
			break
		}
	}
}
