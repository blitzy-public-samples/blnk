/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Blnk represents the CLI application, encapsulating the root Cobra command.
type Blnk struct {
	cmd *cobra.Command // Root command for the CLI application
}

// blnkInstance holds the Blnk instance and its configuration.
type blnkInstance struct {
	blnk *blnk.Blnk            // Blnk object initialized from configuration
	cnf  *config.Configuration // Configuration object holding runtime settings
}

// recoverPanic handles any panics during program execution and logs the error using Logrus.
func recoverPanic() {
	if rec := recover(); rec != nil {
		logrus.Error(rec) // Log the recovered panic
		os.Exit(1)        // Exit the program with an error status
	}
}

// loadInstance loads the configuration file and initializes the Blnk
// instance into app. Extracted from preRun so the initialization sequence
// returns errors instead of exiting, keeping the Fatal at the command layer.
func loadInstance(app *blnkInstance, configFile string, role blnk.ProcessRole) error {
	// Initialize configuration from the specified configuration file.
	if err := config.InitConfig(configFile); err != nil {
		return fmt.Errorf("error loading config: %w", err)
	}

	// Fetch the configuration settings.
	cnf, err := config.Fetch()
	if err != nil {
		return err
	}

	// Initialize the Blnk instance using the fetched configuration.
	newBlnk, err := setupBlnkForRole(cnf, role)
	if err != nil {
		notification.NotifyError(err) // Notify via the internal notification system
		return err
	}

	// Assign the new Blnk instance and configuration to the app struct.
	app.blnk = newBlnk
	app.cnf = cnf

	return nil
}

// preRun sets up the configuration and initializes the Blnk instance before running any command.
func preRun(app *blnkInstance) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := loadInstance(app, "blnk.json", processRoleFor(cmd)); err != nil {
			log.Fatal(err)
		}
		return nil
	}
}

// processRoleFor maps the subcommand being executed to the process role its service container
// should be built for.
func processRoleFor(cmd *cobra.Command) blnk.ProcessRole {
	if cmd == nil {
		return blnk.ProcessRoleServer
	}

	switch cmd.Name() {
	case "workers":
		return blnk.ProcessRoleWorker
	case "migrate", "verify-chain":
		return blnk.ProcessRoleTool
	default:
		return blnk.ProcessRoleServer
	}
}

// setupBlnk creates and initializes a new Blnk instance based on the provided configuration.
func setupBlnk(cfg *config.Configuration) (*blnk.Blnk, error) {
	return setupBlnkForRole(cfg, blnk.ProcessRoleServer)
}

// setupBlnkForRole is setupBlnk with the process role made explicit.
func setupBlnkForRole(cfg *config.Configuration, role blnk.ProcessRole) (*blnk.Blnk, error) {
	// Initialize a new data source from the configuration.
	db, err := database.NewDataSource(cfg)
	if err != nil {
		return &blnk.Blnk{}, fmt.Errorf("error getting datasource: %v", err)
	}

	// Create a new Blnk instance using the initialized data source.
	newBlnk, err := blnk.NewBlnkForRole(db, role)
	if err != nil {
		logrus.Error(err) // Log the error using Logrus
		return &blnk.Blnk{}, fmt.Errorf("error creating blnk: %v", err)
	}
	return newBlnk, nil
}

// NewCLI creates the command-line interface (CLI) for the Blnk application.
func NewCLI() *Blnk {
	var configFile string // Configuration file path (defaults to ./blnk.json)
	b := &blnkInstance{}  // Instance of Blnk to be passed into commands

	// Define the root command with usage and description.
	rootCmd := &cobra.Command{
		Use:   "blnk",
		Short: "Open source ledger",                       // Brief description for the CLI tool
		Run:   func(cmd *cobra.Command, args []string) {}, // Main function for the root command

		// A RUNTIME FAILURE IS NOT A USAGE ERROR. `blnk start` now returns its errors so the
		// stack unwinds through every deferred shutdown instead of calling os.Exit from deep
		// inside the command, and Cobra's default response to a returned error is to print the
		// full usage text after it. For a server that failed to bind a port, or a
		// configuration file that would not parse, that pushes the one line that says what
		// happened above a screen of flag documentation which has nothing to do with it.
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Add a persistent flag to the root command for specifying the config file.
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "./blnk.json", "Configuration file for wallet lite")

	// Set the persistent pre-run hook to initialize the app and config before executing any command.
	rootCmd.PersistentPreRunE = preRun(b)

	// Add various subcommands to the root command.
	rootCmd.AddCommand(serverCommands(b))      // Command for starting the server
	rootCmd.AddCommand(workerCommands(b))      // Command for worker processes
	rootCmd.AddCommand(migrateCommands(b))     // Command for database/schema migrations
	rootCmd.AddCommand(verifyChainCommands(b)) // Command for verifying the transaction hash chain

	return &Blnk{cmd: rootCmd}
}

// executeCLI runs the root command, handling any errors that occur during execution.
func (w Blnk) executeCLI() {
	if err := w.cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err) // Print any errors that occur
		os.Exit(1)                   // Exit the program with an error status
	}
}

// main is the main function and the entry point for the application.
func main() {
	defer recoverPanic() // Ensure that any panic is handled gracefully

	cli := NewCLI()  // Create the CLI application
	cli.executeCLI() // Execute the CLI commands
}
