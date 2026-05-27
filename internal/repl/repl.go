package repl

import "fmt"

type CliCommand struct {
	Name        string
	Description string
	Callback    func(*interface{}, []string) error
}

type cli struct {
	commands map[string]CliCommand
}

func NewCli() cli {
	return cli{
		commands: make(map[string]CliCommand),
	}
}

func (cli *cli) RegisterCLICmd(name, description string, callback func(*interface{}, []string) error) error {

	if _, ok := cli.commands[name]; ok {
		return fmt.Errorf("command %s already registerd", name)
	}

	cli.commands[name] = CliCommand{
		Name:        name,
		Description: description,
		Callback:    callback,
	}

	return nil
}

func (cli *cli) PrintHelp() {

	fmt.Println("Commands available:")
	for _, cmd := range cli.commands {
		fmt.Printf("\t%s\t%s\n", cmd.Name, cmd.Description)
	}
}

func (cli *cli) RunCmd(cmdName string, cfg *interface{}, args []string) error {
	if cmd, ok := cli.commands[cmdName]; ok {
		return cmd.Callback(cfg, args)
	} else {
		return fmt.Errorf("Command %s not found.", cmdName)
	}
}
