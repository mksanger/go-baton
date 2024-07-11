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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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
)

func ParseStdin(logger zerolog.Logger, args []string) (inputContents map[string]interface{}) {
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

func getStringValue(logger zerolog.Logger, object map[string]interface{}, key string, short_key string) (value string, err error) {
	if value = fmt.Sprintf("%+v", object[key]); value != "" && value != "<nil>" {
		return value, nil
	}
	logger.Debug().Msgf("No key %s, looking for short key %s", key, short_key)

	if value = fmt.Sprintf("%+v", object[short_key]); value != "" && value != "<nil>" {
		logger.Info().Msgf("Found %s: %s", key, value)
		return value, nil
	}
	return value, fmt.Errorf("no %s key found: %w", key, ErrMissingKey)
}

func getArrayValue(logger zerolog.Logger, object map[string]interface{}, key string, short_key string) (value []string, err error) {
	value, err = json.Unmarshal(json.Marshal(object[key]))
	return
}

func GetCollectionValue(logger zerolog.Logger, object map[string]interface{}) (string, error) {
	return getStringValue(logger, object, JSON_COLLECTION_KEY, JSON_COLLECTION_SHORT_KEY)
}

func GetDataObjectValue(logger zerolog.Logger, object map[string]interface{}) (string, error) {
	return getStringValue(logger, object, JSON_DATA_OBJECT_KEY, JSON_DATA_OBJECT_SHORT_KEY)
}

func GetiRODSPathValue(logger zerolog.Logger, object map[string]interface{}) (path string, err error) {
	var coll, obj string
	if coll, err = GetCollectionValue(logger, object); err != nil {
		return "", err
	}

	if obj, err = GetDataObjectValue(logger, object); err == ErrMissingKey {
		logger.Debug().Msg("No Data Object key in input json")
		return filepath.Clean(coll), nil
	} else if err != nil {
		return "", err
	}

	return filepath.Clean(fmt.Sprintf("%s/%s", coll, obj)), nil
}

func GetDirectoryValue(logger zerolog.Logger, object map[string]interface{}) (string, error) {
	return getStringValue(logger, object, JSON_DIRECTORY_KEY, JSON_DIRECTORY_SHORT_KEY)
}

func GetFileValue(logger zerolog.Logger, object map[string]interface{}) (string, error) {
	return getStringValue(logger, object, JSON_FILE_KEY, "")
}

func GetLocalPathValue(logger zerolog.Logger, object map[string]interface{}) (path string, err error) {
	var dir, file string
	if dir, err = GetDirectoryValue(logger, object); err != nil {
		return "", err
	}

	if file, err = GetFileValue(logger, object); err == ErrMissingKey {
		logger.Info().Msg("No File key in input json")
		return filepath.Clean(dir), nil
	} else if err != nil {
		return "", err
	}

	return filepath.Clean(fmt.Sprintf("%s/%s", dir, file)), nil
}

func GetAVUsValue(logger zerolog.Logger, object map[string]interface{}) (string, error) {
	return getStringValue(logger, object, JSON_AVUS_KEY, "")
}

func GetMetaValue(logger zerolog.Logger, object map[string]interface{}) (avujson map[string]string, err error) {
	var avus string
	if avus, err = GetAVUsValue(logger, object); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(avus))

	if err = decoder.Decode(&avujson); err != nil {
		logger.Err(err).Msg("Failed to decode avus")
		return nil, err
	}
	return avujson, nil
}
