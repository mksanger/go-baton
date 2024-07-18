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

package cmd

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/cobra"
	"github.com/wtsi-npg/go-baton/appInfo"
	"github.com/wtsi-npg/go-baton/irods"
	"github.com/wtsi-npg/go-baton/parsing"
	"golang.org/x/term"
)

type contextKey string

var mainLogger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})

type cliFlags struct {
	coll      bool
	level     string
	obj       bool
	operation string
	recurse   bool
	zone      string
}

var flags cliFlags

func configureRootLogger(flags *cliFlags) zerolog.Logger {
	var level zerolog.Level

	switch strings.ToLower(flags.level) {
	case "trace":
		level = zerolog.TraceLevel
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	default:
		level = zerolog.InfoLevel
	}

	var writer io.Writer
	if term.IsTerminal(int(os.Stdout.Fd())) {
		writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	} else {
		writer = os.Stderr
	}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	return zerolog.New(zerolog.SyncWriter(writer)).With().
		Timestamp().
		Str("app", appInfo.Name).
		Str("version", appInfo.Version).
		Int("pid", os.Getpid()).
		Logger().Level(level)
}

func printHelp(cmd *cobra.Command, args []string) {
	if err := cmd.Help(); err != nil {
		mainLogger.Error().Err(err).Msg("Help command failed")
		os.Exit(1)
	}
}

func CLI() {
	logger := configureRootLogger(&flags)
	jsonKey := contextKey("json key")
	accountKey := contextKey("account key")
	rootCmd := &cobra.Command{
		Use:     "go-baton",
		Short:   "A go equivalent of baton for testing the go iRODS clients.",
		Run:     printHelp,
		Version: appInfo.Version,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			// Need to print help explicitly or this function will hang waiting for stdin
			if cmd.CalledAs() == "go-baton" {
				printHelp(cmd, args)
				os.Exit(0)
			}
			inputContents := parsing.ParseStdin(logger, args)
			envFile := irods.IRODSEnvFilePath()
			manager, err := irods.NewICommandsEnvironmentManager(logger, envFile)
			if err != nil {
				return err
			}
			account, err := irods.NewIRODSAccount(logger, manager)
			if err != nil {
				return err
			}

			inputctx := context.WithValue(cmd.Context(), jsonKey, inputContents)
			fullctx := context.WithValue(inputctx, accountKey, account)
			cmd.SetContext(fullctx)
			return nil
		},
	}
	rootCmd.PersistentFlags().StringVar(&flags.level,
		"log-level", "info",
		"Set the log level (trace, debug, info, warn, error)")
	rootCmd.SetVersionTemplate(`{{printf "%s\n" .Version}}`)
	putCmd := &cobra.Command{
		Use:   "put",
		Short: "Upload files to iRODS.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return irods.Put(logger, cmd.Context().Value(accountKey).(*types.IRODSAccount), cmd.Context().Value(jsonKey).(map[string]interface{}))
		},
	}

	rootCmd.AddCommand(putCmd)
	//putCmd.Flags().BoolVar(&flags.recurse, "recurse", false, "")

	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Download objects from iRODS.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return irods.Get(logger, cmd.Context().Value(accountKey).(*types.IRODSAccount), cmd.Context().Value(jsonKey).(map[string]interface{}))
		},
	}
	rootCmd.AddCommand(getCmd)

	metaModCmd := &cobra.Command{
		Use:   "metamod",
		Short: "Alter metadata on objects or collections",
		RunE: func(cmd *cobra.Command, args []string) error {
			return irods.MetaMod(logger, cmd.Context().Value(accountKey).(*types.IRODSAccount), cmd.Context().Value(jsonKey).(map[string]interface{}), flags.operation)
		},
	}
	rootCmd.AddCommand(metaModCmd)
	metaModCmd.Flags().StringVar(&flags.operation, "operation", "", "Operation to perform. One of [add, remove]. \nRequired")
	metaModCmd.MarkFlagRequired("operation")

	metaQueryCmd := &cobra.Command{
		Use:   "metaquery",
		Short: "Query object or collection metadata",
		RunE: func(cmd *cobra.Command, args []string) error {
			return irods.MetaQuery(logger, cmd.Context().Value(accountKey).(*types.IRODSAccount), cmd.Context().Value(jsonKey).(map[string]interface{}), flags.zone, flags.coll, flags.obj)
		},
	}
	rootCmd.AddCommand(metaQueryCmd)
	metaQueryCmd.Flags().StringVar(&flags.zone, "zone", "", "Zone in which to perform query. \nRequired")
	metaQueryCmd.Flags().BoolVar(&flags.coll, "collection", false, "Query collection metadata. Default false")
	metaQueryCmd.Flags().BoolVar(&flags.obj, "object", false, "Query object metadata. Default false")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
