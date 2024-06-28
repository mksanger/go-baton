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

// Mostly stolen from sqyrrl setup as go was being difficult about importing it
package irods

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/icommands"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"
	"github.com/wtsi-npg/go-baton/app_info"
)

const (
	IRODSEnvFileDefault = "~/.irods/irods_environment.json"
	IRODSEnvFileEnvVar  = "IRODS_ENVIRONMENT_FILE"
	IRODSPasswordEnvVar = "IRODS_PASSWORD"
	IRODSPublicUser     = "public"
)

// IRODSEnvFilePath returns the path to the iRODS environment file. If the path
// is not set in the environment, the default path is returned.
func IRODSEnvFilePath() string {
	path := os.Getenv(IRODSEnvFileEnvVar)
	if path == "" {
		path = IRODSEnvFileDefault
	}
	path = filepath.Clean(path)

	envRoot, err := os.UserHomeDir()
	if err != nil {
		envRoot = "."
	}
	if path[0] == '~' {
		path = envRoot + path[1:]
	}

	return path
}

// NewICommandsEnvironmentManager creates a new environment manager instance.
//
// This function creates a manager and sets the iRODS environment file path from the
// shell environment. If an iRODS auth file is present, the password is read from it.
// Otherwise, the password is read from the shell environment.
func NewICommandsEnvironmentManager(logger zerolog.Logger,
	iRODSEnvFilePath string) (manager *icommands.ICommandsEnvironmentManager, err error) {
	if iRODSEnvFilePath == "" {
		return nil, fmt.Errorf("iRODS environment file path was empty: %w",
			ErrInvalidArgument)
	}

	// manager.Load() below will succeed even if the iRODS environment file does not
	// exist, but we absolutely don't want that behaviour here.
	var fileInfo os.FileInfo
	if fileInfo, err = os.Stat(iRODSEnvFilePath); err != nil && os.IsNotExist(err) {
		return nil, err
	}
	if fileInfo.IsDir() {
		return nil, fmt.Errorf("iRODS environment file is a directory: %w",
			ErrInvalidArgument)
	}
	if manager, err = icommands.CreateIcommandsEnvironmentManager(); err != nil {
		return nil, err
	}
	if err = manager.SetEnvironmentFilePath(iRODSEnvFilePath); err != nil {
		return nil, err
	}
	if err = manager.Load(os.Getpid()); err != nil {
		return nil, err
	}

	logger.Info().
		Str("path", iRODSEnvFilePath).
		Msg("Loaded iRODS environment file")

	authFilePath := manager.GetPasswordFilePath()

	// An existing auth file takes precedence over the environment variable
	if _, err = os.Stat(authFilePath); err != nil && os.IsNotExist(err) {
		password, ok := os.LookupEnv(IRODSPasswordEnvVar)
		if !ok {
			return nil, fmt.Errorf("iRODS auth file '%s' was not present "+
				"and the '%s' environment variable needed to create it was not set: %w",
				authFilePath, IRODSPasswordEnvVar, ErrMissingArgument)
		}
		if password == "" {
			return nil, fmt.Errorf("iRODS auth file '%s' was not present "+
				"and the '%s' environment variable needed to set it was empty: %w",
				authFilePath, IRODSPasswordEnvVar, ErrInvalidArgument)
		}

		manager.Password = password // manager.Password is propagated to the iRODS account
	}

	return manager, nil
}

// NewIRODSAccount returns an iRODS account instance using the iRODS environment for
// configuration. The environment file path is obtained from the manager.
func NewIRODSAccount(logger zerolog.Logger,
	manager *icommands.ICommandsEnvironmentManager) (account *types.IRODSAccount, err error) { // NRV
	if account, err = manager.ToIRODSAccount(); err != nil {
		logger.Err(err).Msg("Failed to obtain an iRODS account instance")
		return nil, err
	}

	logger.Info().
		Str("host", account.Host).
		Int("port", account.Port).
		Str("zone", account.ClientZone).
		Str("user", account.ClientUser).
		Str("env_file", manager.GetEnvironmentFilePath()).
		Str("auth_file", manager.GetPasswordFilePath()).
		Str("auth_scheme", string(account.AuthenticationScheme)).
		Bool("cs_neg_required", account.ClientServerNegotiation).
		Str("cs_neg_policy", string(account.CSNegotiationPolicy)).
		Str("ca_cert_path", account.SSLConfiguration.CACertificatePath).
		Str("ca_cert_file", account.SSLConfiguration.CACertificateFile).
		Str("enc_alg", account.SSLConfiguration.EncryptionAlgorithm).
		Int("key_size", account.SSLConfiguration.EncryptionKeySize).
		Int("salt_size", account.SSLConfiguration.SaltSize).
		Int("hash_rounds", account.SSLConfiguration.HashRounds).
		Msg("iRODS account created")

	// Before returning the account, check that it is usable by connecting to the
	// iRODS server and accessing the root collection.
	var filesystem *fs.FileSystem
	filesystem, err = fs.NewFileSystemWithDefault(account, app_info.Name)
	if err != nil {
		logger.Err(err).Msg("Failed to create an iRODS file system")
		return nil, err
	}

	var root *fs.Entry
	root, err = filesystem.StatDir("/")
	if err != nil {
		logger.Err(err).Msg("Failed to stat the root zone collection")
		return nil, err
	}
	logger.Debug().
		Str("path", root.Path).
		Msg("Root zone collection is accessible")

	return account, err
}
