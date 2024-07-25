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
	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"

	"github.com/wtsi-npg/go-baton/appInfo"
	"github.com/wtsi-npg/go-baton/parsing"
)

func Chmod(logger zerolog.Logger, account *types.IRODSAccount, jsonContents map[string]interface{}, recurse bool) (err error) {
	var iPath, owner, zone string
	var level types.IRODSAccessLevelType
	var acls []interface{}
	var coll bool
	var aclValue map[string]interface{}
	var conn *connection.IRODSConnection

	if iPath, coll, err = parsing.GetiRODSPath(logger, jsonContents); err != nil {
		return err
	}

	if acls, err = parsing.GetACLList(logger, jsonContents); err != nil {
		return err
	}

	filesystem, err := fs.NewFileSystemWithDefault(account, appInfo.Name)
	if err != nil {
		return err
	}

	defer filesystem.Release()

	if conn, err = filesystem.GetMetadataConnection(); err != nil {
		return err
	}

	conn.Lock()

	defer conn.Unlock()

	for _, acl := range acls {
		if err = parsing.ExtractJSONValue(logger, acl, &aclValue); err != nil {
			return err
		}
		if owner, level, zone, err = parsing.GetACLQuery(logger, aclValue); err != nil {
			return err
		}
		if coll {
			if err = irods_fs.ChangeCollectionAccess(conn, iPath, level, owner, zone, recurse, false); err != nil {
				return err
			}
		} else {
			if err = irods_fs.ChangeDataObjectAccess(conn, iPath, level, owner, zone, false); err != nil {
				return err
			}
		}
		logger.Debug().Msgf("changed permissions on %s for %s to %s", iPath, owner, level)

	}
	return nil
}
