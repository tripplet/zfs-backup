package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/eiannone/keyboard"
)

func main() {
	dataDataset := flag.String("data", "", "Name of the data dataset")
	backupDataset := flag.String("backup", "", "Name of the backup dataset")
	createSnapshot := flag.Bool("snapshot", false, "Create new snapshot")
	noSnapshot := flag.Bool("no-snapshot", false, "Do not create new snapshot")
	flag.Parse()

	// Get existing snapshots from data dataset
	id_cmd := exec.Command("id", "-u")
	output, err := id_cmd.Output()

	if err != nil {
		fmt.Println("Error determining uid:", err)
		os.Exit(-1)
	}

	uid, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		fmt.Println("Error parsing uid:", err)
		os.Exit(-1)
	}

	if uid != 0 {
		fmt.Println("This program must be run as root! (sudo)")
		os.Exit(-1)
	}

	if *dataDataset == "" || *backupDataset == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}

	dataSnapshots, err := getSnapshots(*dataDataset)
	if err != nil {
		os.Exit(-1)
	}

	// Get existing snapshots from backup dataset
	backupSnapshots, err := getSnapshots(*backupDataset)
	if err != nil {
		os.Exit(-1)
	}

	fmt.Println("- Snapshots:")
	printSnapshots(dataSnapshots, backupSnapshots)

	fmt.Println()
	fmt.Println("- Determining snapshot pair for incremental backup...")
	oldSnapshot := findLatestSnapshotPair(dataSnapshots, backupSnapshots)

	if oldSnapshot == "" {
		fmt.Println("    No snapshot pair found")
		os.Exit(-1)
	}

	if !*createSnapshot && !*noSnapshot {
		fmt.Println("- Create new snapshot of data volume? (y/n)")
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

	newSnapshot := getSnapshotName(dataSnapshots[len(dataSnapshots)-1])

	if oldSnapshot == newSnapshot {
		fmt.Println("    No new snapshot found")
		os.Exit(-1)
	}

	fmt.Printf("- Pair for incremental backup\n       %s (old)\n    => %s (new)\n", oldSnapshot, newSnapshot)
	fmt.Println("- Execute the following command:")
	fmt.Printf("    zfs send -i %s@%s %s@%s | pv -pterb | zfs receive -F %s", *dataDataset, oldSnapshot, *dataDataset, newSnapshot, *backupDataset)
	fmt.Println()
	fmt.Println()

	backupBlockDevice := "??"
	backupPool := getPoolNameFromDataset(*backupDataset)
	cmd := exec.Command("zpool", "list", "-v", "-H", "-o", "name", "-L", backupPool)
	cmdOut, err := cmd.CombinedOutput()

	if err == nil {
		lines := strings.Split(strings.Trim(string(cmdOut), " \t\r\n"), "\n")

		// Assume the backup disk is only based on one block device
		if len(lines) == 2 {
			backupBlockDevice = strings.Split(strings.Trim(lines[1], " \t"), "\t")[0]

			lsblkCmd := exec.Command("lsblk", "-no", "pkname", fmt.Sprintf("/dev/%s", backupBlockDevice))
			out, err := lsblkCmd.CombinedOutput()

			if err != nil {
				fmt.Printf("- Executing %s failed with: %s\n", lsblkCmd.Args, err)
			} else {
				backupBlockDevice = strings.Trim(string(out), " \t\r\n")
			}
		}
	}

	if backupBlockDevice == "??" {
		fmt.Println("- Error determining block device:")
		fmt.Println(string(cmdOut))
	}

	fmt.Println("- After the transfer run:")
	fmt.Printf("    zpool sync %s\n", backupPool)
	fmt.Println()

	fmt.Println("- Before removing the backup disk run:")
	fmt.Printf("    zpool export %s\n", backupPool)
	fmt.Println("    sync")
	fmt.Printf("    hdparm -y /dev/%s\n", backupBlockDevice)
	fmt.Printf("    echo 1 > /sys/block/%s/device/delete\n", backupBlockDevice)
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

func findLatestSnapshotPair(dataSnapshots, backupSnapshots []string) string {
	foundSnapShot := ""
	for dataIdx := len(dataSnapshots) - 1; dataIdx > 0; dataIdx-- {
		snapshotName := getSnapshotName(dataSnapshots[dataIdx])
		for backupIdx := len(backupSnapshots) - 1; backupIdx > 0; backupIdx-- {
			if snapshotName == getSnapshotName(backupSnapshots[backupIdx]) {
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

// Get the name of the pool from a dataset or snapshot
//
// Examples:
// data/system@snapshot123 => data
// data123 => data123
// data1/test => data1
func getPoolNameFromDataset(dataset string) string {
	return strings.SplitN(dataset, "/", 2)[0]
}

// Get the name of the snapshot without the leading dataset
//
// Example: data@snapshot123 => snapshot123
func getSnapshotName(dataset string) string {
	return strings.SplitN(dataset, "@", 2)[1]
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
