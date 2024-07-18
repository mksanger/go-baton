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
	"github.com/wtsi-npg/go-baton/appInfo"
	"github.com/wtsi-npg/go-baton/parsing"
)

func MetaMod(logger zerolog.Logger, account *types.IRODSAccount,
	jsonContents map[string]interface{}, operation string) (err error) {
	var iPath string
	var meta []interface{}

	if operation != parsing.JSON_ARG_META_ADD && operation != parsing.JSON_ARG_META_REM {
		return fmt.Errorf("operation argument != %s or %s: %w",
			parsing.JSON_ARG_META_ADD, parsing.JSON_ARG_META_REM, ErrMissingArgument)
	}

	if iPath, err = parsing.GetiRODSPathValue(logger, jsonContents); err != nil {
		return err
	}

	if meta, err = parsing.GetAVUsList(logger, jsonContents); err != nil {
		return err
	}

	filesystem, err := fs.NewFileSystemWithDefault(account, appInfo.Name)
	if err != nil {
		return err
	}

	defer filesystem.Release()
	logger.Info().Msgf("%s %v to %s", operation, meta, iPath)

	for _, metaInterface := range meta {
		var metaValue map[string]interface{}
		if err = parsing.ExtractJSONValue(logger, metaInterface, &metaValue); err != nil {
			return err
		}
		var attr, value, units string
		if attr, value, units, err = parsing.GetAVUValues(logger, metaValue); err != nil {
			return err
		}
		if operation == parsing.JSON_ARG_META_ADD && value != "" {
			if err = filesystem.AddMetadata(iPath, attr, value, units); err != nil {
				logger.Err(err).Msg("Error adding metadata attribute: %s, value: %s, units: %s")
				return err
			}
			logger.Debug().Msgf("Added attribute: %s, value: %s, units: %s to %s", attr, value, units, iPath)
		} else if operation == parsing.JSON_RM_OP || operation == parsing.JSON_ARG_META_REM {
			if err = filesystem.DeleteMetadataByName(iPath, attr); err != nil {
				logger.Err(err).Msg("Error removing metadata attribute: %s, value: %s, units: %s")
				return err
			}
			logger.Debug().Msgf("Removed attribute: %s from %s", attr, iPath)
		} else if value == "" {
			return parsing.ErrMissingKey
		}
	}
	return nil
}
