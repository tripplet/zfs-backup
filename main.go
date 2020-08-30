package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/eiannone/keyboard"
)

const dataPool = "data"
const backupPool = "backup/data"

func main() {
	dataDataset := flag.String("data", "", "Name of the data dataset")
	backupDataset := flag.String("backup", "", "Name of the backup dataset")
	createSnapshot := flag.Bool("snapshot", false, "Create new snapshot")
	noSnapshot := flag.Bool("no-snapshot", false, "Do not create new snapshot")
	flag.Parse()

	if *dataDataset == "" || *backupDataset == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}

	// Get existing snapshots
	dataSnapshots, err := getSnapshots(*dataDataset)
	if err != nil {
		os.Exit(-1)
	}

	backupSnapshots, err := getSnapshots(*backupDataset)
	if err != nil {
		os.Exit(-1)
	}

	fmt.Println("- Snapshots:")

	printSnapshots(dataSnapshots, backupSnapshots)

	fmt.Println()
	fmt.Println("- Determining snapshot pair for incremental backup...")

	oldSnapshot := findLatestSnapshotPair(dataSnapshots, backupSnapshots)

	if !*createSnapshot && !*noSnapshot {
		fmt.Println("- Create new snapshot of data? (y/n)")
		char, _, err := keyboard.GetSingleKey()
		if err != nil {
			os.Exit(-1)
		}

		if char == 'y' || char == 'Y' {
			*createSnapshot = true
		}
	}

	if *createSnapshot {
		fmt.Print("    Creating new snapshot... ")

		cmd := exec.Command("zfs", "snapshot", fmt.Sprintf("%s@%s", *dataDataset, time.Now().Format("20060102.1504")))
		fmt.Printf("    $ %s\n", cmd.Args)

		if cmd.Run() != nil {
			fmt.Printf("    Executing %s failed with: %s\n", cmd.Args, err)
			os.Exit(-1)
		} else {
			fmt.Println("    Done")
		}

		// Get existing snapshots again
		dataSnapshots, err = getSnapshots(*dataDataset)
		if err != nil {
			os.Exit(-1)
		}
	} else {
		fmt.Println("- Skipping snapshot creation")
	}

	newSnapshot := strings.Replace(dataSnapshots[len(dataSnapshots)-1], dataPool, "", 1)

	if oldSnapshot == "" {
		fmt.Printf("    No snapshot pair found")
		os.Exit(-1)
	}

	if oldSnapshot == newSnapshot {
		fmt.Printf("    No new snapshot found")
		os.Exit(-1)
	}

	fmt.Printf("- Pair for incremental backup\n    %s => %s\n", oldSnapshot, newSnapshot)
	fmt.Println("- Execute the following command:")
	fmt.Println()

	fmt.Printf("    zfs send -i %s%s %s%s | pv -pterb | zfs receive -F %s", *dataDataset, oldSnapshot, *dataDataset, newSnapshot, *backupDataset)
	fmt.Println()
	fmt.Println()

	disk := "??"
	cmd := exec.Command("zpool", "list", "-v", "-H", "-o", "name", "-L", *dataDataset)
	cmdOut, err := cmd.CombinedOutput()

	if err == nil {
		lines := strings.Split(strings.Trim(string(cmdOut), " \t\r\n"), "\n")

		// Assume the backup disk is only based on one block device
		if len(lines) == 3 {
			disk = strings.Split(strings.Trim(lines[2], " \t"), "\t")[0]

			// Assume a single partition per disk
			disk = strings.TrimSuffix(disk, "1")
		}
	}

	fmt.Println("- After the transfer run: ")
	fmt.Println()
	fmt.Printf("    zfs export %s\n", *backupDataset)
	fmt.Println("    sync")
	fmt.Printf("    hdparm -y /dev/%s\n", disk)
	fmt.Printf("    echo 1 > /sys/block/%s/device/delete\n", disk)
}

func getSnapshots(volume string) ([]string, error) {
	cmd := exec.Command("zfs", "list", "-H", "-t", "snapshot", "-o", "name", volume)

	fmt.Printf("- Getting snapshots from dataset '%s'...\n", volume)
	fmt.Printf("    $ %s\n", cmd.Args)
	out, err := cmd.CombinedOutput()

	if err != nil {
		fmt.Printf("- Executing %s failed with: %s\n", cmd.Args, err)
		return nil, err
	}

	return strings.Split(strings.Trim(string(out), " \t\r\n"), "\n"), nil
}

func findLatestSnapshotPair(dataSnapshots []string, backupSnapshots []string) string {
	foundSnapShot := ""
	for dataIdx := len(dataSnapshots) - 1; dataIdx > 0; dataIdx-- {
		snapshotName := strings.Replace(dataSnapshots[dataIdx], dataPool, "", 1)

		for backupIdx := len(backupSnapshots) - 1; backupIdx > 0; backupIdx-- {
			if snapshotName == strings.Replace(backupSnapshots[backupIdx], backupPool, "", 1) {
				foundSnapShot = snapshotName
				break
			}
		}

		if foundSnapShot != "" {
			break
		}

	}

	return foundSnapShot
}

func printSnapshots(dataSnapshots []string, backupSnapshots []string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "    Data Dataset\tBackup Dataset")

	for idx := 0; idx < len(dataSnapshots) || idx < len(backupSnapshots); idx++ {
		if idx < len(dataSnapshots) {
			fmt.Fprint(w, "    "+dataSnapshots[idx])
		}

		fmt.Fprint(w, "\t")

		if idx < len(backupSnapshots) {
			fmt.Fprint(w, backupSnapshots[idx])
		}

		fmt.Fprintln(w)
	}

	w.Flush()
}
