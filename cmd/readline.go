/*
Copyright © 2020 Marvin

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
package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wentaojin/tidba/database"
)

var cli *CommandLine

// CommandLine represents the interactive command line structure
type CommandLine struct {
	mutex         *sync.RWMutex
	prompt        string
	readliner     *readline.Instance
	promptColor   *color.Color
	rootCmd       *cobra.Command
	activeCluster string // The name of the cluster where the interactive command line is logged in and active
}

// NewCommandLine creates a new CommandLine instance
func NewCommandLine(rootCmd *cobra.Command, clusterName string, histFile string) (*CommandLine, error) {
	prompt := `tidba »»» `

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            prompt,
		HistoryFile:       histFile,
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
		AutoComplete:      readline.NewPrefixCompleter(GenCompleter(rootCmd)...),
	})
	if err != nil {
		return nil, err
	}

	l := &CommandLine{
		mutex:       &sync.RWMutex{},
		prompt:      prompt,
		readliner:   rl,
		promptColor: color.New(color.FgGreen),
		rootCmd:     rootCmd,
	}

	// the new prompt is set if and only if the cluster persistentFlags is specified and the cluster is logged in.
	if clusterName != "" {
		// login cluster
		_, err := database.Connector.GetDatabase(clusterName)
		if err != nil {
			return nil, err
		}
		l.SetClusterPrompt(clusterName)
	} else {
		l.SetDefaultPrompt()
	}
	return l, nil
}

// SetDefaultPrompt resets the current prompt to the default prompt as
// configured in the config.
func (l *CommandLine) SetDefaultPrompt() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.readliner.SetPrompt(l.promptColor.Sprint(`tidba »»» `))
	l.activeCluster = ""
}

// SetNewPrompt set the command line prompt
func (l *CommandLine) SetClusterPrompt(clusterName string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var b strings.Builder
	b.WriteString(l.promptColor.Sprint("tidba"))
	b.WriteString(color.New(color.FgRed).Sprintf("[%s]", clusterName))
	b.WriteString(l.promptColor.Sprint(" »»» "))
	l.prompt = b.String()
	l.readliner.SetPrompt(l.prompt)
	l.activeCluster = clusterName
}

func (l *CommandLine) GetClusterName() string {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.activeCluster
}

// Run starts the interactive command line
func (l *CommandLine) Run() error {
	defer l.readliner.Close()
	printASCIILogo(l.promptColor)

	var cmds []string
	// each time readline is called, it needs to re-acquire the cobra command
	getRootCmd := func() *cobra.Command {
		rootCmd := Cmd(&App{})

		// disableInteractive default hidden command
		// non-disableInteractive mode disable hidden command, display normal
		for _, subCmd := range rootCmd.Commands() {
			if subCmd.Use == "login" || subCmd.Use == "logout" || subCmd.Use == "clear" {
				subCmd.Hidden = false
			}
			cmds = append(cmds, subCmd.Use)
		}

		// when origin rootCmd persistentFlags changed, loading and new rootCmd setting
		l.rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
			if f.Changed {
				// running mode
				// tidba -c {clusterName} or tidba -> login -c {clusterName}
				rootCmd.PersistentFlags().Set(f.Name, f.Value.String())
			}
		})

		clusterName, err := rootCmd.PersistentFlags().GetString("cluster")
		if err != nil {
			fmt.Printf("the interactive command line get cluster flag failed: %v", err)
			os.Exit(1)
		}

		activeClusterName := l.GetClusterName()

		// when the cluster is logged in, the flag parameter clusterName is automatically set so that the cobra command can be executed normally.
		// tidba -> login -c {clusterName}
		if strings.EqualFold(clusterName, "") && !strings.EqualFold(activeClusterName, "") {
			rootCmd.PersistentFlags().Set("cluster", activeClusterName)
		}

		// hide metadata parameter settings. Only available in non-interactive mode. Cannot be changed in interactive mode.
		rootCmd.LocalFlags().MarkHidden("metadata")
		rootCmd.LocalFlags().MarkHidden("disable-interactive")

		return rootCmd
	}

	for {
		line, err := l.readliner.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}

		line = strings.TrimSpace(line)

		if line == "exit" {
			os.Exit(0)
		}

		if line == "clear" {
			if _, err := readline.ClearScreen(l.readliner); err != nil {
				return err
			}
		}

		args, err := shellwords.Parse(line)
		if err != nil {
			return fmt.Errorf("parse command err: %v", err)
		}

		rootCmd := getRootCmd()

		if line == "" {
			rootCmd.SetArgs([]string{"help"})
			if err := rootCmd.Execute(); err != nil {
				rootCmd.Printf("❌ Execute error: %v\n", err)
			}
			continue
		}

		rootCmd.SetArgs(args)
		rootCmd.ParseFlags(args)
		if err := rootCmd.Execute(); err != nil {
			rootCmd.Printf("❌ Execute error: %v\n", err)
		}
	}
	return nil
}

func printASCIILogo(c *color.Color) {
	newColor := c.SprintFunc()
	fmt.Println(newColor(`Welcome to`))
	fmt.Println(newColor(`	           _______ ___  ___  ___  `))
	fmt.Println(newColor(`	          /_  __(_) _ \/ _ )/ _ |`))
	fmt.Println(newColor(`	           / / / / // / _  / __ |`))
	fmt.Println(newColor(`	          /_/ /_/____/____/_/ |_|`))
	fmt.Println(newColor(`   ____          __  __      ___      ___                  `))
	fmt.Println(newColor(`  / __/__  __ __/ /_/ /     / _ \___ / (_)  _____ ______ __`))
	fmt.Println(newColor(` _\ \/ _ \/ // / __/ _ \   / // / -_) / / |/ / -_) __/ // /`))
	fmt.Println(newColor(`/___/\___/\_,_/\__/_//_/  /____/\__/_/_/|___/\__/_/  \_, / `))
	fmt.Println(newColor(`                                                    /___/  `))
}

func GenCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}
	if len(cmd.Commands()) != 0 {
		for _, v := range cmd.Commands() {
			if v.HasFlags() {
				flagsPc := []readline.PrefixCompleterInterface{}
				flagUsages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\"n")
				for i := 0; i < len(flagUsages)-1; i++ {
					flagsPc = append(flagsPc, readline.PcItem(strings.Split(strings.Trim(flagUsages[i], " "), " ")[0]))
				}
				flagsPc = append(flagsPc, GenCompleter(v)...)
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], flagsPc...))

			} else {
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], GenCompleter(v)...))
			}
		}
	}
	return pc
}
