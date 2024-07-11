/*
 * Copyright (C) 2024. Genome Research Ltd. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License,
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package irods

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"
	"github.com/wtsi-npg/go-baton/app_info"
	"github.com/wtsi-npg/go-baton/parsing"
)

const (
	JSON_ARG_METADATA_ADD = "add"
	JSON_ARG_METADATA_REM = "rem"
)

func MetaMod(logger zerolog.Logger, account *types.IRODSAccount,
	jsonContents map[string]interface{}, operation string) (err error) {
	var iPath string
	var meta map[string]interface{}

	if operation != JSON_ARG_METADATA_ADD && operation != JSON_ARG_METADATA_REM {
		return fmt.Errorf("operation argument != %s or %s: %w",
			JSON_ARG_METADATA_ADD, JSON_ARG_METADATA_REM, ErrMissingArgument)
	}

	if iPath, err = parsing.GetiRODSPathValue(logger, jsonContents); err != nil {
		logger.Err(err)
		return err
	}

	if meta, err = parsing.GetMetaValue(logger, jsonContents); err != nil {
		logger.Err(err)
		return err
	}

	filesystem, err := fs.NewFileSystemWithDefault(account, app_info.Name)
	if err != nil {
		logger.Err(err)
		return err
	}

	defer filesystem.Release()
	logger.Info().Msgf("%s %v to %s", operation, meta, iPath)

	return nil
}
