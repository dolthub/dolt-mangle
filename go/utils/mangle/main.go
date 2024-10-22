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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/fatih/color"
)

func main() {
	ctx := context.Background()

	var fs filesys.Filesys
	fs = filesys.LocalFS

	dataDirFS, err := fs.WithWorkingDir(".")
	if err != nil {
		fmt.Println(color.RedString("Failed to set the data directory. %v", err))
		os.Exit(1)
	}

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, dataDirFS, doltdb.LocalDirDoltDB, doltversion.Version)
	if dEnv.CfgLoadErr != nil {
		fmt.Println(color.RedString("Failed to load the dolt config. %v", dEnv.CfgLoadErr))
		os.Exit(1)
	}
	if dEnv.DBLoadError != nil {
		fmt.Println(color.RedString("Failed to load the database. %v", dEnv.DBLoadError))
		os.Exit(1)
	}

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
	cs := datas.ChunkStoreFromDatabase(db)

	if gs, ok := cs.(*nbs.GenerationalNBS); ok {
		err := nbs.LoopOverTables(ctx, gs)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	} else {
		fmt.Println("Not a generational chunk store")
		os.Exit(1)
	}
}
