// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func LoopOverTables(ctx context.Context, csgs *GenerationalNBS) error {
	stats := Stats{}

	// If the object is in a garbage collection file, we'll need to read it from the oldGen.
	//	gen := csgs.oldGen.tables.upstream
	gen := csgs.newGen.tables.upstream

	vs := types.NewValueStore(csgs)
	for tableId, table := range gen {
		if tableId.String() == chunks.JournalFileID {
			fmt.Println("Skipping Journal File")
			continue
		}

		idx, err := table.index()
		if err != nil {
			return err
		}
		sampleCount := idx.chunkCount()

		objectToHack := hash.Parse("3pdd8aasraqh1tmuedjmcr5nr2fccud2")

		hackedPersister, err := NewCmpChunkTableWriter("")
		if err != nil {
			return err
		}

		for i := uint32(0); i < sampleCount; i++ {
			var h hash.Hash
			_, err := idx.indexEntry(i, &h)
			if err != nil {
				return err
			}
			// We'll read Chunks, then recompress them into CompressedChunks
			rawData, err := table.get(ctx, h, &stats)
			if err != nil {
				return err
			}

			chnk := chunks.NewChunkWithHash(h, rawData)

			if h == objectToHack {
				// Deserialize the chunk.
				origVal, err := types.DecodeValue(chnk, vs)
				if err != nil {
					return err
				}

				hackedVal := origVal

				if isCmt, _ := datas.IsCommit(origVal); isCmt {
					commit, err := serial.TryGetRootAsCommit([]byte(origVal.(types.SerialMessage)), serial.MessagePrefixSz)
					if err != nil {
						return err
					}

					rootAddr := hash.New(commit.RootBytes())

					parentCnt := commit.ParentAddrsLength() / hash.ByteLen
					addrs := commit.ParentAddrsBytes()
					parents := make([]hash.Hash, 0, parentCnt)
					for i := 0; i < parentCnt; i++ {
						addr := hash.New(addrs[:hash.ByteLen])
						addrs = addrs[hash.ByteLen:]
						parents = append(parents, addr)
					}

					opts := datas.CommitOptions{Meta: &datas.CommitMeta{
						Name:          string(commit.Name()),
						Email:         string(commit.Email()),
						Timestamp:     commit.TimestampMillis() - 300000, // Make it look like the commit was made 5 min before it actually was.
						UserTimestamp: commit.UserTimestampMillis() - 300000,
						Description:   string(commit.Description()),
					}, Parents: parents}

					parentClosure := hash.New(commit.ParentClosureBytes())

					heights := make([]uint64, 1)
					heights[0] = commit.Height() - 1

					altMsg, _ := datas.ExposeCommitFlatbuffer(rootAddr, opts, heights, parentClosure)
					hackedVal = types.SerialMessage(altMsg)
					chnk = chunks.NewChunkWithHash(objectToHack, []byte(hackedVal.(types.SerialMessage)))
				} else {
					return fmt.Errorf("object type is not a commit: %s", origVal.HumanReadableString())
				}

				fmt.Println("----------------------- MANGLE -----------------------------")
				fmt.Println(fmt.Sprintf("Found object %s in Table File: %s", h.String(), tableId))
				fmt.Println(origVal.HumanReadableString())
				fmt.Println("ALTERED TO:")
				fmt.Println(hackedVal.HumanReadableString())
				fmt.Println("------------------------------------------------------------")
			}

			cmpChnk := ChunkToCompressedChunk(chnk)

			err = hackedPersister.AddCmpChunk(cmpChnk)
			if err != nil {
				return err
			}
		}
		_, err = hackedPersister.Finish()
		if err != nil {
			return err
		}

		err = hackedPersister.FlushToFile(fmt.Sprintf("%s.hacked", tableId))
		if err != nil {
			return err
		}
	}

	return nil
}
