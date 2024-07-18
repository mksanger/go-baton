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
package parsing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
)

const (
	// File/directory, data object/collection, properties
	JSON_ZONE_KEY              = "zone"
	JSON_DIRECTORY_KEY         = "directory"
	JSON_DIRECTORY_SHORT_KEY   = "dir"
	JSON_FILE_KEY              = "file"
	JSON_COLLECTION_KEY        = "collection"
	JSON_COLLECTION_SHORT_KEY  = "coll"
	JSON_DATA_OBJECT_KEY       = "data_object"
	JSON_DATA_OBJECT_SHORT_KEY = "obj"
	JSON_DATA_KEY              = "data"
	JSON_CONTENTS_KEY          = "contents"
	JSON_SIZE_KEY              = "size"
	JSON_CHECKSUM_KEY          = "checksum"
	JSON_TIMESTAMPS_KEY        = "timestamps"
	JSON_TIMESTAMPS_SHORT_KEY  = "time"

	// Permissions
	JSON_ACCESS_KEY = "access"
	JSON_OWNER_KEY  = "owner"
	JSON_LEVEL_KEY  = "level"

	// Metadata attributes, values
	JSON_AVUS_KEY            = "avus"
	JSON_ATTRIBUTE_KEY       = "attribute"
	JSON_ATTRIBUTE_SHORT_KEY = "a"
	JSON_VALUE_KEY           = "value"
	JSON_VALUE_SHORT_KEY     = "v"
	JSON_UNITS_KEY           = "units"
	JSON_UNITS_SHORT_KEY     = "u"

	JSON_CREATED_KEY        = "created"
	JSON_CREATED_SHORT_KEY  = "c"
	JSON_MODIFIED_KEY       = "modified"
	JSON_MODIFIED_SHORT_KEY = "m"

	// Metadata genquery API operations
	JSON_OPERATOR_KEY       = "operator"
	JSON_OPERATOR_SHORT_KEY = "o"
	JSON_ARGS_KEY           = "args"
	JSON_ARGS_SHORT_KEY     = "?"
	JSON_ARG_META_ADD       = "add"
	JSON_ARG_META_REM       = "rem"

	// SQL specific query operations
	JSON_SPECIFIC_KEY  = "specific"
	JSON_SQL_KEY       = "sql"
	JSON_SQL_SHORT_KEY = "s"

	// baton operations
	JSON_TARGET_KEY          = "target"
	JSON_RESULT_KEY          = "result"
	JSON_SINGLE_RESULT_KEY   = "single"
	JSON_MULTIPLE_RESULT_KEY = "multiple"
	JSON_OP_KEY              = "operation"
	JSON_OP_SHORT_KEY        = "op"

	JSON_CHMOD_OP     = "chmod"
	JSON_CHECKSUM_OP  = "checksum"
	JSON_GET_OP       = "get"
	JSON_LIST_OP      = "list"
	JSON_METAMOD_OP   = "metamod"
	JSON_METAQUERY_OP = "metaquery"
	JSON_PUT_OP       = "put"
	JSON_MOVE_OP      = "move"
	JSON_RM_OP        = "remove"
	JSON_MKCOLL_OP    = "mkdir"
	JSON_RMCOLL_OP    = "rmdir"

	JSON_OP_ARGS_KEY       = "arguments"
	JSON_OP_ARGS_SHORT_KEY = "args"

	JSON_OP_ACL           = "acl"
	JSON_OP_AVU           = "avu"
	JSON_OP_CHECKSUM      = "checksum"
	JSON_OP_VERIFY        = "verify"
	JSON_OP_FORCE         = "force"
	JSON_OP_COLLECTION    = "collection"
	JSON_OP_CONTENTS      = "contents"
	JSON_OP_OBJECT        = "object"
	JSON_OP_OPERATION     = "operation"
	JSON_OP_RAW           = "raw"
	JSON_OP_RECURSE       = "recurse"
	JSON_OP_REPLICATE     = "replicate"
	JSON_OP_SAVE          = "save"
	JSON_OP_SINGLE_SERVER = "single-server"
	JSON_OP_SIZE          = "size"
	JSON_OP_TIMESTAMP     = "timestamp"
	JSON_OP_PATH          = "path"

	VALID_REPLICATE   = "1"
	INVALID_REPLICATE = "0"
)

func ParseStdin(logger zerolog.Logger, args []string) (
	inputContents map[string]interface{}) {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		logger.Err(err).Msg("Failed to read stdin")
		os.Exit(74)
	}

	err = json.Unmarshal(input, &inputContents)
	if err != nil {
		logger.Err(err).Msg("Failed to decode json")
		os.Exit(1)
	}
	return inputContents
}

func ExtractJSONValue(logger zerolog.Logger, value interface{}, extracted any) (
	err error) {
	var marshalled []byte
	if marshalled, err = json.Marshal(value); err != nil {
		logger.Err(err).Msg("Error remarshalling value from json")
		return err
	}

	if err = json.Unmarshal(marshalled, &extracted); err != nil {
		logger.Err(err).Msg("Error unmarshalling value from json")
		return err
	}
	return nil
}

func getStringValue(logger zerolog.Logger, object map[string]interface{},
	key string, short_key string) (value string, err error) {
	if err = ExtractJSONValue(logger, object[key], &value); err != nil {
		return "", err
	}
	if value == "" {
		logger.Debug().Msgf("No key %s, looking for short key %s", key, short_key)
		if err = ExtractJSONValue(logger, object[short_key], &value); err != nil {
			return "", err
		}
	}

	if value == "" {
		return value, fmt.Errorf("no %s key found: %w", key, ErrMissingKey)
	}
	logger.Info().Msgf("Found %s: %s", key, value)
	return value, nil

}

func GetCollectionValue(logger zerolog.Logger, object map[string]interface{}) (
	string, error) {
	return getStringValue(logger, object, JSON_COLLECTION_KEY, JSON_COLLECTION_SHORT_KEY)
}

func GetDataObjectValue(logger zerolog.Logger, object map[string]interface{}) (
	string, error) {
	return getStringValue(logger, object, JSON_DATA_OBJECT_KEY, JSON_DATA_OBJECT_SHORT_KEY)
}

func GetiRODSPathValue(logger zerolog.Logger, object map[string]interface{}) (
	path string, err error) {
	var coll, obj string
	if coll, err = GetCollectionValue(logger, object); err != nil {
		return "", err
	}

	if obj, err = GetDataObjectValue(logger, object); errors.Is(err, ErrMissingKey) {
		logger.Debug().Msg("No Data Object key in input json")
		return filepath.Clean(coll), nil
	} else if err != nil {
		return "", err
	}

	return filepath.Clean(fmt.Sprintf("%s/%s", coll, obj)), nil
}

func GetDirectoryValue(logger zerolog.Logger, object map[string]interface{}) (
	string, error) {
	return getStringValue(logger, object, JSON_DIRECTORY_KEY, JSON_DIRECTORY_SHORT_KEY)
}

func GetFileValue(logger zerolog.Logger, object map[string]interface{}) (string, error) {
	return getStringValue(logger, object, JSON_FILE_KEY, "")
}

func GetLocalPathValue(logger zerolog.Logger, object map[string]interface{}) (
	path string, err error) {
	var dir, file string
	if dir, err = GetDirectoryValue(logger, object); err != nil {
		return "", err
	}

	if file, err = GetFileValue(logger, object); errors.Is(err, ErrMissingKey) {
		logger.Info().Msg("No File key in input json")
		return filepath.Clean(dir), nil
	} else if err != nil {
		return "", err
	}

	return filepath.Clean(fmt.Sprintf("%s/%s", dir, file)), nil
}

func GetAVUsList(logger zerolog.Logger, object map[string]interface{}) (
	avus []interface{}, err error) {
	if err = ExtractJSONValue(logger, object[JSON_AVUS_KEY], &avus); err != nil {
		return nil, err
	}
	return avus, nil
}

func GetAVUValues(logger zerolog.Logger, object map[string]interface{}) (
	attr string, value string, units string, err error) {
	if attr, err = getStringValue(
		logger, object, JSON_ATTRIBUTE_KEY, JSON_ATTRIBUTE_SHORT_KEY,
	); err != nil {
		return "", "", "", err
	}

	// value is not required for del
	if value, err = getStringValue(
		logger, object, JSON_VALUE_KEY, JSON_VALUE_SHORT_KEY,
	); err != nil && !errors.Is(err, ErrMissingKey) {
		return "", "", "", err
	}

	// units are optional always
	if units, err = getStringValue(
		logger, object, JSON_UNITS_KEY, JSON_UNITS_SHORT_KEY,
	); err != nil && !errors.Is(err, ErrMissingKey) {
		return "", "", "", err
	}
	return attr, value, units, nil
}

func GetAVUQuery(logger zerolog.Logger, object map[string]interface{}) (
	attr string, value string, op string, err error) {
	if attr, value, _, err = GetAVUValues(logger, object); err != nil {
		return "", "", "", err
	}

	// operator defaults to equals
	if op, err = getStringValue(logger, object, JSON_OPERATOR_KEY,
		JSON_OPERATOR_SHORT_KEY); err != nil && !errors.Is(err, ErrMissingKey) {
		return "", "", "", err
	}

	return attr, value, op, nil
}
