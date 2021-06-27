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

	isRoot, err := isRootUser()
	if err != nil {
		exitWithError("Error checking for root:", err)
	} else if !isRoot {
		exitWithError("This program must be run as root! (sudo)")
	}

	if *dataDataset == "" || *backupDataset == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}

	// Get existing snapshots from data dataset
	dataSnapshots, dataError := getSnapshots(*dataDataset)
	if dataError != nil {
		fmt.Println(dataError)
	}

	// Get existing snapshots from backup dataset
	backupSnapshots, backupError := getSnapshots(*backupDataset)
	if backupError != nil {
		fmt.Println(backupError)
	}

	fmt.Println("- Snapshots:")
	printSnapshots(dataSnapshots, backupSnapshots)

	if dataError != nil || backupError != nil {
		os.Exit(-1)
	}

	fmt.Println()
	fmt.Println("- Determining snapshot pair for incremental backup...")
	oldSnapshot := findLatestSnapshotPair(dataSnapshots, backupSnapshots)

	if oldSnapshot == "" {
		exitWithError("    No snapshot pair found")
	} else {
		fmt.Println("    found")
	}

	if !*createSnapshot && !*noSnapshot {
		fmt.Println("- Create new snapshot of data volume? (y/n)")
		char, _, err := keyboard.GetSingleKey()
		if err != nil {
			exitWithError(err)
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
			exitWithError(fmt.Sprintf("    Executing %s failed with: %s\n", cmd.Args, err))
		} else {
			fmt.Println("    Done")
		}

		// Get existing snapshots again
		dataSnapshots, err = getSnapshots(*dataDataset)
		if err != nil {
			exitWithError(err)
		}
	} else {
		fmt.Println("- Skipping snapshot creation")
	}

	newSnapshot := getSnapshotName(dataSnapshots[len(dataSnapshots)-1])

	if oldSnapshot == newSnapshot {
		exitWithError("    No new snapshot found")
	}

	fmt.Printf("- Pair for incremental backup\n       %s (old)\n    => %s (new)\n", oldSnapshot, newSnapshot)

	cmd := exec.Command("zfs", "send", "--dryrun", "--parsable", "-i", fmt.Sprintf("%s@%s", *dataDataset, oldSnapshot), fmt.Sprintf("%s@%s", *dataDataset, newSnapshot))
	cmdOut, err := cmd.CombinedOutput()
	if err != nil {
		exitWithError(err)
	}

	estimatedSizeLine := strings.Split(strings.Trim(string(cmdOut), " \t\r\n"), "\n")[1]
	estimatedSizeStr := strings.Trim(strings.Split(estimatedSizeLine, "\t")[1], " \t")
	estimatedSize, err := strconv.ParseInt(estimatedSizeStr, 10, 64)
	if err != nil {
		exitWithError(err)
	}

	fmt.Println("- Execute the following command:")
	fmt.Printf("    zfs send -i %s@%s %s@%s | pv -pterb -s %d | zfs receive -F %s", *dataDataset, oldSnapshot, *dataDataset, newSnapshot, estimatedSize, *backupDataset)
	fmt.Println()

	fmt.Printf("    (estimated size %s)", toHumanByteFormat(estimatedSize))
	fmt.Println()
	fmt.Println()

	backupBlockDevice := "??"
	backupPool := getPoolNameFromDataset(*backupDataset)
	cmd = exec.Command("zpool", "list", "-v", "-H", "-o", "name", "-L", backupPool)
	cmdOut, err = cmd.CombinedOutput()

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
		return nil, fmt.Errorf("- Executing %s failed with: %s\n", cmd.Args, err)
	}

	return strings.Split(strings.Trim(string(out), " \t\r\n"), "\n"), nil
}

func toHumanByteFormat(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
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

func isRootUser() (bool, error) {
	id_cmd := exec.Command("id", "-u")

	output, err := id_cmd.Output()
	if err != nil {
		return false, fmt.Errorf("Error determining uid:", err)
	}

	uid, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return false, fmt.Errorf("Error parsing uid:", err)
	}

	return uid == 0, nil
}

func exitWithError(v ...interface{}) {
	fmt.Println(v...)
	os.Exit(-1)
}
