// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build influxdb2_test
// +build influxdb2_test

// To run unit test: go test -tags influxdb2_test

package influxdb2

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	protocol "github.com/influxdata/line-protocol"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/cadvisor/cmd/internal/storage/test"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The duration in seconds for which stats will be buffered in the influxdb driver.
const kCacheDuration = 1

type influxDbTestStorageDriver struct {
	count  int
	buffer int
	base   storage.StorageDriver
}

func (self *influxDbTestStorageDriver) readyToFlush() bool {
	return self.count >= self.buffer
}

func (self *influxDbTestStorageDriver) AddStats(cInfo *info.ContainerInfo, stats *info.ContainerStats) error {
	self.count++
	return self.base.AddStats(cInfo, stats)
}

func (self *influxDbTestStorageDriver) Close() error {
	return self.base.Close()
}

func (self *influxDbTestStorageDriver) StatsEq(a, b *info.ContainerStats) bool {
	if !test.TimeEq(a.Timestamp, b.Timestamp, 10*time.Millisecond) {
		return false
	}
	// Check only the stats populated in influxdb.
	if !reflect.DeepEqual(a.Cpu.Usage, b.Cpu.Usage) {
		return false
	}

	if a.Memory.Usage != b.Memory.Usage {
		return false
	}

	if a.Memory.WorkingSet != b.Memory.WorkingSet {
		return false
	}

	if !reflect.DeepEqual(a.Network, b.Network) {
		return false
	}

	if !reflect.DeepEqual(a.Filesystem, b.Filesystem) {
		return false
	}
	return true
}

func createTestStorage(org, bucket, token string) (*influxdbStorage2, error) {
	machineName, err := os.Hostname()
	// Store the URL of your InfluxDB instance

	var host string
	if os.Getenv("APP_ENV") == "" {
		// 非容器环境
		host = "localhost:8086"
	} else {
		// 容器环境
		host = "influxdb:8086"
	}
	//username := "admin"

	drv, err := newStorage(machineName, bucket, org, token, host, false, 2*time.Minute)

	return drv, err
}

func runStorageTest(f func(test.TestStorageDriver, *testing.T), t *testing.T, bufferCount int) {
	org := "cadvisor_db"
	bucket := "cadvisor_table"
	token := "Iob-DZFTuQ75Cylijz_NFs5s0ci2FQXiJIIk4XTXCXH-h9jYeN_AQ3mFSOMhuGzkcjdIav8FzIg-hds46b-Axw=="
	host := "localhost:8086"

	driver, err := createTestStorage(org, bucket, token)
	if err != nil {
		t.Fatal(err)
	}
	defer func(driver *influxdbStorage2) {
		err := driver.Close()
		if err != nil {
			t.Fatal("关闭influxdb客户端错误: " + err.Error())
		}
	}(driver)

	testDriver := &influxDbTestStorageDriver{buffer: bufferCount}
	driver.OverrideReadyToFlush(testDriver.readyToFlush)
	testDriver.base = driver

	// Generate another container's data on same machine.
	test.StorageDriverFillRandomStatsFunc("containerOnSameMachine", 100, testDriver, t)

	// Generate another container's data on another machine.
	driverForAnotherMachine, err := newStorage("machineB", bucket, org, token, host, false,
		time.Duration(bufferCount))
	if err != nil {
		t.Fatal(err)
	}
	defer func(driverForAnotherMachine *influxdbStorage2) {
		err := driverForAnotherMachine.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(driverForAnotherMachine)
	testDriverOtherMachine := &influxDbTestStorageDriver{buffer: bufferCount}
	driverForAnotherMachine.OverrideReadyToFlush(testDriverOtherMachine.readyToFlush)
	testDriverOtherMachine.base = driverForAnotherMachine

	test.StorageDriverFillRandomStatsFunc("containerOnAnotherMachine", 100, testDriverOtherMachine, t)
	f(testDriver, t)
}

func TestContainerFileSystemStatsToPoints(t *testing.T) {
	assert := assert.New(t)

	machineName := "testMachine"
	org := "cadvisor_db"
	bucket := "cadvisor_test"
	token := "Iob-DZFTuQ75Cylijz_NFs5s0ci2FQXiJIIk4XTXCXH-h9jYeN_AQ3mFSOMhuGzkcjdIav8FzIg-hds46b-Axw=="
	host := "localhost:8086"

	storage, err := newStorage(machineName, bucket, org, token, host, false, 2*time.Minute)
	assert.Nil(err)

	cInfo := &info.ContainerInfo{
		ContainerReference: info.ContainerReference{
			Name: "containerName",
		},
	}

	stats := &info.ContainerStats{}
	points := storage.containerFilesystemStatsToPoints(cInfo, stats)

	// stats.Filesystem is always nil, not sure why
	assert.Nil(points)
}

func TestContainerStatsToPoints(t *testing.T) {
	// Given
	org := "cadvisor_db"
	bucket := "cadvisor_table"
	token := "Iob-DZFTuQ75Cylijz_NFs5s0ci2FQXiJIIk4XTXCXH-h9jYeN_AQ3mFSOMhuGzkcjdIav8FzIg-hds46b-Axw=="

	storage, err := createTestStorage(org, bucket, token)
	require.Nil(t, err)
	require.NotNil(t, storage)

	cInfo, stats := createTestStats()
	require.Nil(t, err)
	require.NotNil(t, stats)

	// When
	points := storage.containerStatsToPoints(cInfo, stats)
	points = append(points, storage.memoryStatsToPoints(cInfo, stats)...)
	points = append(points, storage.hugetlbStatsToPoints(cInfo, stats)...)
	points = append(points, storage.perfStatsToPoints(cInfo, stats)...)
	points = append(points, storage.resctrlStatsToPoints(cInfo, stats)...)

	// Then
	assert.NotEmpty(t, points)
	assert.Len(t, points, 34+len(stats.Cpu.Usage.PerCpu))

	// CPU stats
	assertContainsPointWithValue(t, points, serCPUUsageTotal, stats.Cpu.Usage.Total)
	assertContainsPointWithValue(t, points, serCPUUsageSystem, stats.Cpu.Usage.System)
	assertContainsPointWithValue(t, points, serCPUUsageUser, stats.Cpu.Usage.User)
	assertContainsPointWithValue(t, points, serLoadAverage, stats.Cpu.LoadAverage)
	for _, cpu_usage := range stats.Cpu.Usage.PerCpu {
		assertContainsPointWithValue(t, points, serCPUUsagePerCPU, cpu_usage)
	}

	// Memory stats
	assertContainsPointWithValue(t, points, serMemoryUsage, stats.Memory.Usage)
	assertContainsPointWithValue(t, points, serMemoryMaxUsage, stats.Memory.MaxUsage)
	assertContainsPointWithValue(t, points, serMemoryCache, stats.Memory.Cache)
	assertContainsPointWithValue(t, points, serMemoryRss, stats.Memory.RSS)
	assertContainsPointWithValue(t, points, serMemorySwap, stats.Memory.Swap)
	assertContainsPointWithValue(t, points, serMemoryMappedFile, stats.Memory.MappedFile)
	assertContainsPointWithValue(t, points, serMemoryUsage, stats.Memory.Usage)
	assertContainsPointWithValue(t, points, serMemoryWorkingSet, stats.Memory.WorkingSet)
	assertContainsPointWithValue(t, points, serMemoryFailcnt, stats.Memory.Failcnt)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.ContainerData.Pgfault)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.ContainerData.Pgmajfault)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.HierarchicalData.Pgfault)
	assertContainsPointWithValue(t, points, serMemoryFailure, stats.Memory.HierarchicalData.Pgmajfault)

	// Hugetlb stats
	for _, hugetlbStat := range stats.Hugetlb {
		assertContainsPointWithValue(t, points, setHugetlbUsage, hugetlbStat.Usage)
		assertContainsPointWithValue(t, points, setHugetlbMaxUsage, hugetlbStat.MaxUsage)
		assertContainsPointWithValue(t, points, setHugetlbFailcnt, hugetlbStat.Failcnt)
	}

	// Network stats
	assertContainsPointWithValue(t, points, serRxBytes, stats.Network.RxBytes)
	assertContainsPointWithValue(t, points, serRxErrors, stats.Network.RxErrors)
	assertContainsPointWithValue(t, points, serTxBytes, stats.Network.TxBytes)
	assertContainsPointWithValue(t, points, serTxBytes, stats.Network.TxErrors)

	// Perf stats
	for _, perfStat := range stats.PerfStats {
		assertContainsPointWithValue(t, points, serPerfStat, perfStat.Value)
	}

	// Reference memory
	assertContainsPointWithValue(t, points, serReferencedMemory, stats.ReferencedMemory)

	// Resource Control stats - memory bandwidth
	for _, rdtMemoryBandwidth := range stats.Resctrl.MemoryBandwidth {
		assertContainsPointWithValue(t, points, serResctrlMemoryBandwidthTotal, rdtMemoryBandwidth.TotalBytes)
		assertContainsPointWithValue(t, points, serResctrlMemoryBandwidthLocal, rdtMemoryBandwidth.LocalBytes)
	}

	// Resource Control stats - cache
	for _, rdtCache := range stats.Resctrl.Cache {
		assertContainsPointWithValue(t, points, serResctrlLLCOccupancy, rdtCache.LLCOccupancy)
	}
}

func findValInFields(fields []*protocol.Field, key string, value interface{}) bool {
	found := false
	for _, field := range fields {
		if field.Key == key && field.Value == convertField(value) {
			found = true
			break
		}
	}
	return found
}

func assertContainsPointWithValue(t *testing.T, points []*write.Point, name string, value interface{}) bool {
	found := false
	for _, point := range points {
		//if point.Measurement == name && point.Fields[fieldValue] == toSignedIfUnsigned(value) {
		if point.Name() == name && findValInFields(point.FieldList(), name, value) {
			found = true
			break

		}
	}
	return assert.True(t, found, "no point found with name='%v' and value=%v", name, value)
}

func createTestStats() (*info.ContainerInfo, *info.ContainerStats) {
	cInfo := &info.ContainerInfo{
		ContainerReference: info.ContainerReference{
			Name:    "testContainername",
			Aliases: []string{"testContainerAlias1", "testContainerAlias2"},
		},
	}

	cpuUsage := info.CpuUsage{
		Total:  uint64(rand.Intn(10000)),
		PerCpu: []uint64{uint64(rand.Intn(1000)), uint64(rand.Intn(1000)), uint64(rand.Intn(1000))},
		User:   uint64(rand.Intn(10000)),
		System: uint64(rand.Intn(10000)),
	}

	stats := &info.ContainerStats{
		Timestamp: time.Now(),
		Cpu: info.CpuStats{
			Usage:       cpuUsage,
			LoadAverage: int32(rand.Intn(1000)),
		},
		Memory: info.MemoryStats{
			Usage:            26767396864,
			MaxUsage:         30429605888,
			Cache:            7837376512,
			RSS:              18930020352,
			Swap:             1024,
			MappedFile:       1025327104,
			WorkingSet:       23630012416,
			Failcnt:          1,
			ContainerData:    info.MemoryStatsMemoryData{Pgfault: 100328455, Pgmajfault: 97},
			HierarchicalData: info.MemoryStatsMemoryData{Pgfault: 100328454, Pgmajfault: 96},
		},
		Hugetlb: map[string]info.HugetlbStats{
			"1GB": {Usage: 1234, MaxUsage: 5678, Failcnt: 9},
			"2GB": {Usage: 9876, MaxUsage: 5432, Failcnt: 1},
		},
		ReferencedMemory: 12345,
		PerfStats:        []info.PerfStat{{Cpu: 1, PerfValue: info.PerfValue{Name: "cycles", ScalingRatio: 1.5, Value: 4589}}},
		Resctrl: info.ResctrlStats{
			MemoryBandwidth: []info.MemoryBandwidthStats{
				{TotalBytes: 11234, LocalBytes: 4567},
				{TotalBytes: 55678, LocalBytes: 9876},
			},
			Cache: []info.CacheStats{
				{LLCOccupancy: 3},
				{LLCOccupancy: 5},
			},
		},
	}
	return cInfo, stats
}

func TestStorage(t *testing.T) {
	runStorageTest(func(driver test.TestStorageDriver, t *testing.T) {}, t, 1000)
}

func TestInfluxdbStorage2_Ping(t *testing.T) {
	//bucket := "cadvisor_table"
	//org := "cadvisor_test"
	token := "Yymc5-Q0subkRH6_US6rZRlsnlbGLfVf-920JTu0hmwOI7I1tj4foUcZn64ON5Z5fWm2yLGBxKUrFOhqDS0Z9Q=="
	// Store the URL of your InfluxDB instance
	var url string
	if os.Getenv("APP_ENV") == "" {
		// 非容器环境
		url = "http://localhost:8086"
	} else {
		// 容器环境
		url = "http://influxdb:8086"
	}

	opts := influxdb2.DefaultOptions()
	opts.HTTPOptions().SetHTTPDoer(&UserAgentSetter{
		UserAgent:   "FM Container Monitor/1.1",
		RequestDoer: http.DefaultClient,
	})

	//Create client with customized options
	client := influxdb2.NewClientWithOptions(url, token, opts)

	// Always close client at the end
	defer client.Close()

	// Issue a call with custom User-Agent header
	resp, err := client.Ping(context.Background())
	if err != nil {
		panic(err)
	}
	if resp {
		fmt.Println("Server is up")
	} else {
		fmt.Println("Server is down")
	}

}

func TestRunStorage2(t *testing.T) {
	org := "cadvisor"
	bucket := "cadvisor_bucket"
	token := "Iob-DZFTuQ75Cylijz_NFs5s0ci2FQXiJIIk4XTXCXH-h9jYeN_AQ3mFSOMhuGzkcjdIav8FzIg-hds46b-Axw=="
	// Store the URL of your InfluxDB instance

	var url string
	if os.Getenv("APP_ENV") == "" {
		// 非容器环境
		url = "http://localhost:8086"
	} else {
		// 容器环境
		url = "http://influxdb:8086"
	}

	client := influxdb2.NewClient(url, token)
	// User blocking write client for writes to desired bucket
	writeAPI := client.WriteAPIBlocking(org, bucket)
	// Create point using full params constructor
	p := influxdb2.NewPoint("stat",
		map[string]string{"unit": "temperature"},
		map[string]interface{}{"avg": 24.5, "max": 45, "b": 15, "c": 25, "d": 23.6, "e": 33.3},
		time.Now())
	// Write point immediately
	err := writeAPI.WritePoint(context.Background(), p)
	require.Nil(t, err)
	// Ensures background processes finishes
	client.Close()

}

func TestInfluxdbStorage2_AddStats(t *testing.T) {
	// Given
	org := "cadvisor_db"
	bucket := "cadvisor_table_stats"
	token := "L8DCXN5sHUTI-hXE42Kh2-cDYmmQ_ehTkdEPacu2apijEhLgXT-EEvgDjZ3uJU0_oY2Cnf07Q1S1mdmDbbBtYg=="
	storage, err := createTestStorage(org, bucket, token)
	require.Nil(t, err)
	require.NotNil(t, storage)

	cInfo, stats := createTestStats()
	require.Nil(t, err)
	require.NotNil(t, stats)

	// When
	points := storage.containerStatsToPoints(cInfo, stats)
	points = append(points, storage.memoryStatsToPoints(cInfo, stats)...)
	points = append(points, storage.hugetlbStatsToPoints(cInfo, stats)...)
	points = append(points, storage.perfStatsToPoints(cInfo, stats)...)
	points = append(points, storage.resctrlStatsToPoints(cInfo, stats)...)

	err = storage.WriteAPIBlocking.WritePoint(context.Background(), points...)
	require.Nil(t, err)
}
