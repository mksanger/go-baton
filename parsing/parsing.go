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

func ParseStdin(args []string, logger zerolog.Logger) map[string]string {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		logger.Err(err).Msg("Failed to read stdin")
		os.Exit(74)
	}

	var inputContents map[string]string
	err = json.Unmarshal(input, &inputContents)
	if err != nil {
		logger.Err(err).Msg("Failed to decode json")
		os.Exit(1)
	}
	return inputContents
}

func getStringValue(object map[string]string, key string, short_key string,
	logger zerolog.Logger) (value string, err error) {
	if value = object[key]; value == "" && short_key != "" {
		logger.Debug().Msgf("No key %s, looking for short key %s", key, short_key)
		value = object[short_key]
	}

	if value == "" {
		return value, fmt.Errorf("no %s key found: %w", key, ErrMissingKey)
	}
	logger.Info().Msgf("Found %s: %s", key, value)
	return value, nil
}

func GetCollectionValue(object map[string]string, logger zerolog.Logger) (string, error) {
	return getStringValue(object, JSON_COLLECTION_KEY, JSON_COLLECTION_SHORT_KEY, logger)
}

func GetDataObjectValue(object map[string]string, logger zerolog.Logger) (string, error) {
	return getStringValue(object, JSON_DATA_OBJECT_KEY, JSON_DATA_OBJECT_SHORT_KEY, logger)
}

func GetiRODSPathValue(object map[string]string, logger zerolog.Logger) (path string, err error) {
	var coll, obj string
	if coll, err = GetCollectionValue(object, logger); err != nil {
		return "", err
	}

	if obj, err = GetDataObjectValue(object, logger); err == ErrMissingKey {
		logger.Debug().Msg("No Data Object key in input json")
		return filepath.Clean(coll), nil
	} else if err != nil {
		return "", err
	}

	return filepath.Clean(fmt.Sprintf("%s/%s", coll, obj)), nil
}

func GetDirectoryValue(object map[string]string, logger zerolog.Logger) (string, error) {
	return getStringValue(object, JSON_DIRECTORY_KEY, JSON_DIRECTORY_SHORT_KEY, logger)
}

func GetFileValue(object map[string]string, logger zerolog.Logger) (string, error) {
	return getStringValue(object, JSON_FILE_KEY, "", logger)
}

func GetLocalPathValue(object map[string]string, logger zerolog.Logger) (path string, err error) {
	var dir, file string
	if dir, err = GetDirectoryValue(object, logger); err != nil {
		return "", err
	}

	if file, err = GetFileValue(object, logger); err == ErrMissingKey {
		logger.Info().Msg("No File key in input json")
		return filepath.Clean(dir), nil
	} else if err != nil {
		return "", err
	}

	return filepath.Clean(fmt.Sprintf("%s/%s", dir, file)), nil
}
