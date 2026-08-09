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
// This is used to store the runtime instance and configuration globally within the application.
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
//
// The role is threaded through to setupBlnk because it decides one thing: whether this
// process builds a Kafka producer. See blnk.ProcessRole.
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
// It ensures that the configuration is loaded, and the Blnk instance is initialized properly.
//
// The role is derived from the SUBCOMMAND being executed, which is the only place it can be
// derived from: the service container is built here, in PersistentPreRunE, before any
// subcommand's own Run has started. Cobra passes the command it is about to run, so
// processRoleFor turns "workers" into the non-publishing role and everything else into the
// server one.
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
//
// # Why the mapping is by command name
//
// The container is constructed in PersistentPreRunE, which runs before the subcommand's Run,
// so the role cannot be a parameter of the subcommand. Cobra hands PersistentPreRunE the
// command it is about to execute, and its Name() is exactly the discriminator needed.
//
// # Only "workers" is named, and the default is the server
//
// `start` is the server. `migrate` and `verify-chain` are one-shot tools that publish nothing,
// and they reach the same non-publishing decision by a different route: with no relay and no
// producer call, their publisher is never used whichever one they hold. They are mapped to the
// tool role anyway so the intent is recorded rather than incidental.
//
// A command this function does not recognise resolves to the SERVER role, which is the
// compatible answer rather than the safe-by-default one — it is what NewBlnk has always done,
// so an unmapped command behaves exactly as it did before roles existed. The narrower default
// would be the tool role, and it was rejected because it would silently disable publishing for
// a future command that needed it, which is a far quieter failure than an unnecessary
// producer. blnk.ProcessRole.PublishesEvents remains the allowlist for what a role may do; this
// is only the mapping onto it.
//
// Parameters:
//   - cmd *cobra.Command: the command being executed. May be nil, which resolves to the
//     server role for the reason above.
//
// Returns:
//   - blnk.ProcessRole: the role to build the service container for.
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
// It connects to the data source (such as a database) using the configuration settings.
//
// It builds the SERVER role. Tests use it directly and expect the publishing container;
// production goes through setupBlnkForRole with the role the subcommand implies.
func setupBlnk(cfg *config.Configuration) (*blnk.Blnk, error) {
	return setupBlnkForRole(cfg, blnk.ProcessRoleServer)
}

// setupBlnkForRole is setupBlnk with the process role made explicit.
//
// The role reaches blnk.NewBlnkForRole and decides one thing: whether this process builds a
// Kafka producer. The datasource, and every other member of the service container, is
// identical in every role.
//
// Parameters:
//   - cfg *config.Configuration: the loaded configuration.
//   - role blnk.ProcessRole: the role this process runs as.
//
// Returns:
//   - *blnk.Blnk: the service container. A non-nil empty value on error, matching the
//     existing contract.
//   - error: a datasource or construction failure.
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
// It sets up the root command and subcommands like serverCommands, workerCommands, and migrateCommands.
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
		//
		// SilenceErrors goes with it, and for a reason that is easy to get backwards: it does
		// NOT discard the error. executeCLI below already prints whatever Execute returns to
		// stderr and exits non-zero, so leaving Cobra's own reporting on printed the same
		// sentence twice — once as "Error: <msg>" and once bare. One report, from the one
		// place that also decides the exit status.
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
// It serves as the main entry point for the CLI application.
func (w Blnk) executeCLI() {
	if err := w.cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err) // Print any errors that occur
		os.Exit(1)                   // Exit the program with an error status
	}
}

// main is the main function and the entry point for the application.
// It recovers from any panic, initializes the CLI, and executes it.
func main() {
	defer recoverPanic() // Ensure that any panic is handled gracefully

	cli := NewCLI()  // Create the CLI application
	cli.executeCLI() // Execute the CLI commands
}
