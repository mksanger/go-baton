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
	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"
	"github.com/wtsi-npg/go-baton/appInfo"
	"github.com/wtsi-npg/go-baton/parsing"
)

func Put(logger zerolog.Logger, account *types.IRODSAccount, jsonContents map[string]interface{}, calculateChecksum bool) (err error) {
	var iPath, lPath string
	var coll, dir bool
	if iPath, coll, err = parsing.GetiRODSPath(logger, jsonContents); err != nil {
		logger.Err(err)
		return err
	}

	if lPath, dir, err = parsing.GetLocalPath(logger, jsonContents); err != nil {
		logger.Err(err)
		return err
	}
	if dir && !coll {
		err = parsing.ErrMissingKey
		logger.Err(err).Msg("iRODS path for directory put should not be data object")
		return err
	}
	logger.Info().Msgf("Uploading %s to %s", lPath, iPath)

	filesystem, err := fs.NewFileSystemWithDefault(account, appInfo.Name)
	if err != nil {
		logger.Err(err)
		return err
	}

	defer filesystem.Release()

	if err = filesystem.UploadFile(lPath, iPath, "", true, calculateChecksum, true, func(processed int64, total int64) {}); err != nil {
		return err
	}
	logger.Debug().Msgf("Uploaded %s to %s", lPath, iPath)
	return nil
}
