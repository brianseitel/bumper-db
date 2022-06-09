/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"net/http"

	"github.com/brianseitel/bumper/internal/bumper"
	"github.com/brianseitel/bumper/internal/server"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the database server",
	Long:  `Starts the database server on port 8080.`,
	Run: func(cmd *cobra.Command, args []string) {
		bumperDB := bumper.New("./data")
		bumperDB.InitDB()

		// TODO: split files when hits a certain size
		// TODO: support multiple processes
		// TODO: implement GC to clean up deleted data from files

		logger, _ := zap.NewDevelopment()
		logger = zap.NewNop()
		server := server.Controller{
			Logger:   logger,
			BumperDB: bumperDB,
		}

		router := mux.NewRouter()

		server.Register(router)

		n := negroni.Classic() // Includes some default middlewares
		n.UseHandler(router)

		logger.Sugar().Infof("Listening on port 8080")
		http.ListenAndServe(":8080", n)
	},
}

func init() {
	rootCmd.AddCommand(startCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// startCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// startCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
