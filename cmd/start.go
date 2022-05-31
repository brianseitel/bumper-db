/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"net/http"

	"github.com/brianseitel/shard/internal/server"
	"github.com/brianseitel/shard/internal/shard"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		shardDB := shard.New("./data")
		shardDB.InitDB()

		// TODO: Implement key deletion
		// TODO: support non-strings (ints, bytes)
		// TODO: hint files
		// TODO: split files when hits a certain size
		// TODO: support multiple processes
		// TODO: implement GC to clean up deleted data from files

		logger, _ := zap.NewDevelopment()
		server := server.Controller{
			Logger:  logger,
			ShardDB: shardDB,
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
