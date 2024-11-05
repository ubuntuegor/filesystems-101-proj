package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:          "gcsintro",
	SilenceUsage: true,
}

var uploadObj = &cobra.Command{
	Use:  "obj",
	RunE: mainUploadObj,
}

var uploadObjArgs struct {
	bucket string
	size   string
	repeat int
}

func init() {
	uploadObj.Flags().StringVarP(&uploadObjArgs.bucket, "bucket", "b", "", "destination bucket")
	uploadObj.Flags().StringVarP(&uploadObjArgs.size, "size", "s", "4KB", "file size")
	uploadObj.Flags().IntVarP(&uploadObjArgs.repeat, "repeat", "r", 5, "repetitions")
	rootCmd.AddCommand(uploadObj)
}

func mainUploadObj(self *cobra.Command, args []string) error {
	if uploadObjArgs.bucket == "" {
		return errors.New("destination bucket must be specified")
	}

	size, err := datasize.ParseString(uploadObjArgs.size)
	if err != nil {
		return err
	}

	sizeBytes := size.Bytes()
	repetitions := uploadObjArgs.repeat

	measurements := make([]float64, repetitions)

	for i := 0; i < repetitions; i++ {
		time, err := runUploadObj(uploadObjArgs.bucket, int(sizeBytes))
		if err != nil {
			return err
		}

		bps := float64(sizeBytes) / time.Seconds()
		measurements[i] = bps

		bps_size := datasize.ByteSize(bps)
		fmt.Printf("repetition %d\ttime %s\tspeed %s/s\n", i+1, time.String(), bps_size.HumanReadable())
	}

	printSpeedAndVariance(repetitions, measurements)

	return nil
}

func runUploadObj(bucket string, size int) (time.Duration, error) {
	ctx := context.Background()

	c, err := NewGcsClient(ctx)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	buf := makeRandBuf(size)
	if err = c.UploadObject(ctx, bucket, "x", buf); err != nil {
		return 0, err
	}

	return time.Since(start), nil
}

var uploadMultipartObj = &cobra.Command{
	Use:  "mobj",
	RunE: mainUploadMultipartObj,
}

var uploadMultipartObjArgs struct {
	bucket    string
	chunkSize string
	repeat    int
}

func init() {
	uploadMultipartObj.Flags().StringVarP(&uploadMultipartObjArgs.bucket, "bucket", "b", "", "destination bucket")
	uploadMultipartObj.Flags().StringVarP(&uploadMultipartObjArgs.chunkSize, "chunk", "c", "256KB", "chunk size")
	uploadMultipartObj.Flags().IntVarP(&uploadMultipartObjArgs.repeat, "repeat", "r", 5, "repetitions")
	rootCmd.AddCommand(uploadMultipartObj)
}

func mainUploadMultipartObj(self *cobra.Command, args []string) (err error) {
	if uploadMultipartObjArgs.bucket == "" {
		return errors.New("destination bucket must be specified")
	}

	chunkSize, err := datasize.ParseString(uploadMultipartObjArgs.chunkSize)
	if err != nil {
		return err
	}

	chunkSizeBytes := chunkSize.Bytes()
	sizeBytes := 2 * chunkSizeBytes
	repetitions := uploadMultipartObjArgs.repeat

	measurements := make([]float64, repetitions)

	for i := 0; i < repetitions; i++ {
		time, err := runUploadMultipartObj(uploadMultipartObjArgs.bucket, int(chunkSizeBytes))
		if err != nil {
			return err
		}

		bps := float64(sizeBytes) / time.Seconds()
		measurements[i] = bps

		bps_size := datasize.ByteSize(bps)
		fmt.Printf("repetition %d\ttime %s\tspeed %s/s\n", i+1, time.String(), bps_size.HumanReadable())
	}

	printSpeedAndVariance(repetitions, measurements)

	return nil
}

func runUploadMultipartObj(bucket string, chunkSize int) (time.Duration, error) {
	ctx := context.Background()

	c, err := NewGcsClient(ctx)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	uploadUrl, err := c.NewUploadSession(ctx, bucket, "x")
	if err != nil {
		return 0, err
	}

	off, buf := int64(0), makeRandBuf(2*chunkSize)

	if err = c.UploadObjectPart(ctx, uploadUrl, off, buf[:chunkSize], false); err != nil {
		return 0, err
	}
	off += int64(chunkSize)
	buf = buf[chunkSize:]

	testOff, testLast, err := c.GetResumeOffset(ctx, uploadUrl)
	if err != nil {
		return 0, err
	}
	fmt.Printf("GetResumeOffset() = %d, %t\n", testOff, testLast)

	if err = c.UploadObjectPart(ctx, uploadUrl, off, buf[:chunkSize], true); err != nil {
		return 0, err
	}

	testOff, testLast, err = c.GetResumeOffset(ctx, uploadUrl)
	if err != nil {
		return 0, err
	}
	fmt.Printf("GetResumeOffset() = %d, %t\n", testOff, testLast)

	return time.Since(start), nil
}

func printSpeedAndVariance(repetitions int, measurements []float64) {
	totalSpeed := float64(0)
	for i := 0; i < repetitions; i++ {
		totalSpeed += measurements[i]
	}

	avgSpeed := totalSpeed / float64(repetitions)
	avgSpeed_size := datasize.ByteSize(avgSpeed)

	variance := float64(0)
	for i := 0; i < repetitions; i++ {
		variance += (measurements[i] - avgSpeed) * (measurements[i] - avgSpeed)
	}
	variance /= float64(repetitions)
	variance = math.Sqrt(variance)
	variance_size := datasize.ByteSize(variance)

	fmt.Printf("avg speed %s/s\tvariance %s/s\n", avgSpeed_size.HumanReadable(), variance_size.HumanReadable())
}
