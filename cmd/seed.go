/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

// seedCmd represents the seed command
var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		raw, _ := ioutil.ReadFile("/usr/share/dict/propernames")

		lines := strings.Split(string(raw), "\n")

		for _, line := range lines {
			payload := fmt.Sprintf(`{
				"key": "%s",
				"value": "%s"
			}`, line, reverse(line))
			buf := bytes.NewBuffer([]byte(payload))
			req, _ := http.NewRequest("PUT", "http://localhost:8080/v1/database", buf)
			req.Header.Set("Content-Type", "application/json")
			client := http.Client{}

			_, err := client.Do(req)
			if err != nil {
				panic(err)
			}
		}
	},
}

func reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func init() {
	rootCmd.AddCommand(seedCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// seedCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// seedCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
