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
	"encoding/json"
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"
	"github.com/wtsi-npg/go-baton/appInfo"
	"github.com/wtsi-npg/go-baton/parsing"
)

func BuildMetaQuery(logger zerolog.Logger, avus []interface{},
	columns parsing.MetaQueryColumns, zone string) (
	request *message.IRODSMessageQueryRequest, err error,
) {
	var attr, op, val string

	query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, 0, 0, 0)
	query.AddKeyVal(common.ZONE_KW, zone)
	for _, column := range columns.ReturnColumns {
		query.AddSelect(column, 1)
	}

	for _, avu := range avus {
		var avujson map[string]interface{}
		if err := parsing.ExtractJSONValue(logger, avu, &avujson); err != nil {
			return nil, err
		}
		if attr, val, op, err = parsing.GetAVUQuery(logger, avujson); err != nil {
			return nil, err
		}

		attrCondition := fmt.Sprintf("= '%s'", attr)
		valueCondition := fmt.Sprintf("%s '%s'", op, val)
		query.AddCondition(columns.AttributeCondition, attrCondition)
		query.AddCondition(columns.ValueCondition, valueCondition)
	}
	return query, nil
}

func MetaQuery(logger zerolog.Logger, account *types.IRODSAccount,
	jsonContents map[string]interface{}, zone string, collections bool,
	objects bool) (err error) {
	var avus []interface{}
	var conn *connection.IRODSConnection
	var query *message.IRODSMessageQueryRequest
	var response, jsonOut []interface{}

	if !collections && !objects {
		//To match behaviour of baton
		collections = true
		objects = true
	}
	//if account.ClientZone != zone {
	//	logger.Debug().Msgf("Changing zone from %s to %s", account.ClientZone, zone)
	//	if account, err = types.CreateIRODSAccount(
	//		account.Host, account.Port, account.ClientUser,
	//		zone, account.AuthenticationScheme, account.Password,
	//		account.DefaultResource); err != nil {
	//		return err
	//	}
	//}

	if avus, err = parsing.GetAVUsList(logger, jsonContents); err != nil {
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

	if collections {
		collectionColumns := parsing.MetaQueryColumns{
			AttributeCondition: common.ICAT_COLUMN_META_COLL_ATTR_NAME,
			ValueCondition:     common.ICAT_COLUMN_META_COLL_ATTR_VALUE,
			ReturnColumns:      []common.ICATColumnNumber{common.ICAT_COLUMN_COLL_NAME},
			JSONKeys:           []string{parsing.JSON_COLLECTION_KEY},
		}
		if query, err = BuildMetaQuery(logger, avus, collectionColumns, zone); err != nil {
			return err
		}
		queryResult := message.IRODSMessageQueryResponse{}
		if err := conn.Request(query, &queryResult, nil); err != nil {
			logger.Err(err).Msg("Error while querying iRODS")
			return err
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				logger.Info().Msgf("No collections found with metadata: %s", avus)
			} else {
				logger.Err(err).Msg("Error while querying iRODS")
				return err
			}
		} else if queryResult.RowCount == 0 {
			logger.Info().Msgf("No collections found with metadata: %s", avus)
		}

		if response, err = parsing.IRODSXMLToJSON(logger, queryResult, collectionColumns); err != nil {
			return err
		}
		jsonOut = append(jsonOut, response...)

	}

	if objects {
		objectColumns := parsing.MetaQueryColumns{
			AttributeCondition: common.ICAT_COLUMN_META_DATA_ATTR_NAME,
			ValueCondition:     common.ICAT_COLUMN_META_DATA_ATTR_VALUE,
			ReturnColumns:      []common.ICATColumnNumber{common.ICAT_COLUMN_COLL_NAME, common.ICAT_COLUMN_DATA_NAME},
			JSONKeys:           []string{parsing.JSON_COLLECTION_KEY, parsing.JSON_DATA_OBJECT_KEY},
		}
		if query, err = BuildMetaQuery(logger, avus, objectColumns, zone); err != nil {
			return err
		}
		queryResult := message.IRODSMessageQueryResponse{}
		if err := conn.Request(query, &queryResult, nil); err != nil {
			logger.Err(err).Msg("Error while querying iRODS")
			return err
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				logger.Info().Msgf("No data objects found with metadata: %s", avus)
			} else {
				logger.Err(err).Msg("Error while querying iRODS")
				return err
			}
		} else if queryResult.RowCount == 0 {
			logger.Info().Msgf("No data objects found with metadata: %s", avus)
		}

		if response, err = parsing.IRODSXMLToJSON(logger, queryResult, objectColumns); err != nil {
			return err
		}
		jsonOut = append(jsonOut, response...)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.Encode(jsonOut)

	return nil
}
