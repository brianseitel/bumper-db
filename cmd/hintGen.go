/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"github.com/brianseitel/bumper/internal/bumper"
	"github.com/spf13/cobra"
)

// hintGenCmd represents the hintGen command
var hintGenCmd = &cobra.Command{
	Use:   "hintGen",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		bumperDB := bumper.New("./data")
		bumperDB.InitDB()

		bumperDB.GenerateHintFiles()
	},
}

func init() {
	rootCmd.AddCommand(hintGenCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// hintGenCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// hintGenCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
