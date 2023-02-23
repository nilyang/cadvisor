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

package influxdb2

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/cadvisor/version"
	"github.com/influxdata/influxdb-client-go/v2/api"
	ihttp "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	"github.com/influxdata/influxdb-client-go/v2"
)

func init() {
	storage.RegisterStorageDriver("influxdb2", new)
}

var (
	argOrg       = flag.String("storage_driver_influxdb2_org", "cadvisor", "Influxdb v2 organization name")
	argBucket    = flag.String("storage_driver_influxdb2_bucket", "cadvisor_bucket", "Influxdb v2 index bucket name")
	argAuthToken = flag.String("storage_driver_influxdb2_auth_token", "API TOKEN", "Influxdb v2 Api Token")
)

//var argDbRetentionPolicy = flag.String("storage_driver_influxdb_retention_policy", "", "retention policy")

type influxdbStorage2 struct {
	client           influxdb2.Client
	machineName      string
	orgName          string
	writeAPI         api.WriteAPI
	WriteAPIBlocking api.WriteAPIBlocking
	retentionPolicy  string
	bufferDuration   time.Duration
	lastWrite        time.Time
	points           []*write.Point
	lock             sync.Mutex
	readyToFlush     func() bool
}

// UserAgentSetter is the implementation of Doer interface for setting User-Agent header
type UserAgentSetter struct {
	UserAgent   string
	RequestDoer ihttp.Doer
}

// Do fulfills the Doer interface
func (u *UserAgentSetter) Do(req *http.Request) (*http.Response, error) {
	// Set User-Agent header to request
	req.Header.Set("User-Agent", u.UserAgent)
	// Call original Doer to proceed with request
	return u.RequestDoer.Do(req)
}

// Series names
const (
	// Cumulative CPU usage
	serCPUUsageTotal  string = "cpu_usage_total"
	serCPUUsageSystem string = "cpu_usage_system"
	serCPUUsageUser   string = "cpu_usage_user"
	serCPUUsagePerCPU string = "cpu_usage_per_cpu"
	// Smoothed average of number of runnable threads x 1000.
	serLoadAverage string = "load_average"
	// Memory Usage
	serMemoryUsage string = "memory_usage"
	// Maximum memory usage recorded
	serMemoryMaxUsage string = "memory_max_usage"
	// //Number of bytes of page cache memory
	serMemoryCache string = "memory_cache"
	// Size of RSS
	serMemoryRss string = "memory_rss"
	// Container swap usage
	serMemorySwap string = "memory_swap"
	// Size of memory mapped files in bytes
	serMemoryMappedFile string = "memory_mapped_file"
	// Working set size
	serMemoryWorkingSet string = "memory_working_set"
	// Number of memory usage hits limits
	serMemoryFailcnt string = "memory_failcnt"
	// Cumulative count of memory allocation failures
	serMemoryFailure string = "memory_failure"
	// Cumulative count of bytes received.
	serRxBytes string = "rx_bytes"
	// Cumulative count of receive errors encountered.
	serRxErrors string = "rx_errors"
	// Cumulative count of bytes transmitted.
	serTxBytes string = "tx_bytes"
	// Cumulative count of transmit errors encountered.
	serTxErrors string = "tx_errors"
	// Filesystem limit.
	serFsLimit string = "fs_limit"
	// Filesystem usage.
	serFsUsage string = "fs_usage"
	// Hugetlb stat - current res_counter usage for hugetlb
	setHugetlbUsage = "hugetlb_usage"
	// Hugetlb stat - maximum usage ever recorded
	setHugetlbMaxUsage = "hugetlb_max_usage"
	// Hugetlb stat - number of times hugetlb usage allocation failure
	setHugetlbFailcnt = "hugetlb_failcnt"
	// Perf statistics
	serPerfStat = "perf_stat"
	// Referenced memory
	serReferencedMemory = "referenced_memory"
	// Resctrl - Total memory bandwidth
	serResctrlMemoryBandwidthTotal = "resctrl_memory_bandwidth_total"
	// Resctrl - Local memory bandwidth
	serResctrlMemoryBandwidthLocal = "resctrl_memory_bandwidth_local"
	// Resctrl - Last level cache usage
	serResctrlLLCOccupancy = "resctrl_llc_occupancy"
)

func new() (storage.StorageDriver, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return newStorage(
		hostname,
		*argBucket,    // bucket
		*argOrg,       // org
		*argAuthToken, // authToken
		*storage.ArgDbHost,
		*storage.ArgDbIsSecure,
		*storage.ArgDbBufferDuration,
	)
}

// Field names
const (
	fieldValue  string = "value"
	fieldType   string = "type"
	fieldDevice string = "device"
)

// Tag names
const (
	tagMachineName   string = "machine"
	tagContainerName string = "container_name"
)

func (s *influxdbStorage2) containerFilesystemStatsToPoints(
	cInfo *info.ContainerInfo,
	stats *info.ContainerStats) (points []*write.Point) {
	if len(stats.Filesystem) == 0 {
		return points
	}
	for _, fsStat := range stats.Filesystem {

		pointFsUsage := influxdb2.NewPoint(serFsUsage,
			map[string]string{fsStat.Device: serFsUsage},
			map[string]interface{}{fsStat.Device: fsStat.Usage},
			time.Now())

		pointFsLimit := influxdb2.NewPoint(serFsLimit,
			map[string]string{fsStat.Device: serFsLimit},
			map[string]interface{}{fsStat.Device: fsStat.Limit},
			time.Now())
		points = append(points, pointFsUsage, pointFsLimit)
	}

	s.tagPoints(cInfo, stats, points)

	return points
}

// Set tags and timestamp for all points of the batch.
// Points should inherit the tags that are set for BatchPoints, but that does not seem to work.
func (s *influxdbStorage2) tagPoints(cInfo *info.ContainerInfo, stats *info.ContainerStats, points []*write.Point) {
	// Use container alias if possible
	var containerName string
	if len(cInfo.ContainerReference.Aliases) > 0 {
		containerName = cInfo.ContainerReference.Aliases[0]
	} else {
		containerName = cInfo.ContainerReference.Name
	}

	commonTags := map[string]string{
		tagMachineName:   s.machineName,
		tagContainerName: containerName,
	}
	for i := 0; i < len(points); i++ {
		// merge with existing tags if any
		addTagsToPoint(points[i], commonTags)
		addTagsToPoint(points[i], cInfo.Spec.Labels)
		points[i].SetTime(stats.Timestamp)
	}
}

func (s *influxdbStorage2) containerStatsToPoints(
	cInfo *info.ContainerInfo,
	stats *info.ContainerStats,
) (points []*write.Point) {
	// CPU usage: Total usage in nanoseconds
	points = append(points, makePoint(serCPUUsageTotal, stats.Cpu.Usage.Total))

	// CPU usage: Time spend in system space (in nanoseconds)
	points = append(points, makePoint(serCPUUsageSystem, stats.Cpu.Usage.System))

	// CPU usage: Time spent in user space (in nanoseconds)
	points = append(points, makePoint(serCPUUsageUser, stats.Cpu.Usage.User))

	// CPU usage per CPU
	for i := 0; i < len(stats.Cpu.Usage.PerCpu); i++ {
		point := makePoint(serCPUUsagePerCPU, stats.Cpu.Usage.PerCpu[i])
		tags := map[string]string{"instance": fmt.Sprintf("%v", i)}
		addTagsToPoint(point, tags)

		points = append(points, point)
	}

	// Load Average
	points = append(points, makePoint(serLoadAverage, stats.Cpu.LoadAverage))

	// Network Stats
	points = append(points, makePoint(serRxBytes, stats.Network.RxBytes))
	points = append(points, makePoint(serRxErrors, stats.Network.RxErrors))
	points = append(points, makePoint(serTxBytes, stats.Network.TxBytes))
	points = append(points, makePoint(serTxErrors, stats.Network.TxErrors))

	// Referenced Memory
	points = append(points, makePoint(serReferencedMemory, stats.ReferencedMemory))

	s.tagPoints(cInfo, stats, points)

	return points
}

func (s *influxdbStorage2) memoryStatsToPoints(
	cInfo *info.ContainerInfo,
	stats *info.ContainerStats,
) (points []*write.Point) {
	// Memory Usage
	points = append(points, makePoint(serMemoryUsage, stats.Memory.Usage))
	// Maximum memory usage recorded
	points = append(points, makePoint(serMemoryMaxUsage, stats.Memory.MaxUsage))
	//Number of bytes of page cache memory
	points = append(points, makePoint(serMemoryCache, stats.Memory.Cache))
	// Size of RSS
	points = append(points, makePoint(serMemoryRss, stats.Memory.RSS))
	// Container swap usage
	points = append(points, makePoint(serMemorySwap, stats.Memory.Swap))
	// Size of memory mapped files in bytes
	points = append(points, makePoint(serMemoryMappedFile, stats.Memory.MappedFile))
	// Working Set Size
	points = append(points, makePoint(serMemoryWorkingSet, stats.Memory.WorkingSet))
	// Number of memory usage hits limits
	points = append(points, makePoint(serMemoryFailcnt, stats.Memory.Failcnt))

	// Cumulative count of memory allocation failures
	memoryFailuresTags := map[string]string{
		"failure_type": "pgfault",
		"scope":        "container",
	}
	memoryFailurePoint := makePoint(serMemoryFailure, stats.Memory.ContainerData.Pgfault)
	addTagsToPoint(memoryFailurePoint, memoryFailuresTags)
	points = append(points, memoryFailurePoint)

	memoryFailuresTags["failure_type"] = "pgmajfault"
	memoryFailurePoint = makePoint(serMemoryFailure, stats.Memory.ContainerData.Pgmajfault)
	addTagsToPoint(memoryFailurePoint, memoryFailuresTags)
	points = append(points, memoryFailurePoint)

	memoryFailuresTags["failure_type"] = "pgfault"
	memoryFailuresTags["scope"] = "hierarchical"
	memoryFailurePoint = makePoint(serMemoryFailure, stats.Memory.HierarchicalData.Pgfault)
	addTagsToPoint(memoryFailurePoint, memoryFailuresTags)
	points = append(points, memoryFailurePoint)

	memoryFailuresTags["failure_type"] = "pgmajfault"
	memoryFailurePoint = makePoint(serMemoryFailure, stats.Memory.HierarchicalData.Pgmajfault)
	addTagsToPoint(memoryFailurePoint, memoryFailuresTags)
	points = append(points, memoryFailurePoint)

	s.tagPoints(cInfo, stats, points)

	return points
}

func (s *influxdbStorage2) hugetlbStatsToPoints(
	cInfo *info.ContainerInfo,
	stats *info.ContainerStats,
) (points []*write.Point) {

	for pageSize, hugetlbStat := range stats.Hugetlb {
		tags := map[string]string{
			"page_size": pageSize,
		}

		// Hugepage usage
		point := makePoint(setHugetlbUsage, hugetlbStat.Usage)
		addTagsToPoint(point, tags)
		points = append(points, point)

		//Maximum hugepage usage recorded
		point = makePoint(setHugetlbMaxUsage, hugetlbStat.MaxUsage)
		addTagsToPoint(point, tags)
		points = append(points, point)

		// Number of hugepage usage hits limits
		point = makePoint(setHugetlbFailcnt, hugetlbStat.Failcnt)
		addTagsToPoint(point, tags)
		points = append(points, point)
	}

	s.tagPoints(cInfo, stats, points)

	return points
}

func (s *influxdbStorage2) perfStatsToPoints(
	cInfo *info.ContainerInfo,
	stats *info.ContainerStats,
) (points []*write.Point) {

	for _, perfStat := range stats.PerfStats {
		point := makePoint(serPerfStat, perfStat.Value)
		tags := map[string]string{
			"cpu":           fmt.Sprintf("%v", perfStat.Cpu),
			"name":          perfStat.Name,
			"scaling_ratio": fmt.Sprintf("%v", perfStat.ScalingRatio),
		}
		addTagsToPoint(point, tags)
		points = append(points, point)
	}

	s.tagPoints(cInfo, stats, points)

	return points
}

func (s *influxdbStorage2) resctrlStatsToPoints(
	cInfo *info.ContainerInfo,
	stats *info.ContainerStats,
) (points []*write.Point) {

	// Memory bandwidth
	for nodeID, rdtMemoryBandwidth := range stats.Resctrl.MemoryBandwidth {
		tags := map[string]string{
			"node_id": fmt.Sprintf("%v", nodeID),
		}
		point := makePoint(serResctrlMemoryBandwidthTotal, rdtMemoryBandwidth.TotalBytes)
		addTagsToPoint(point, tags)
		points = append(points, point)

		point = makePoint(serResctrlMemoryBandwidthLocal, rdtMemoryBandwidth.LocalBytes)
		addTagsToPoint(point, tags)
		points = append(points, point)
	}

	// Cache
	for nodeID, rdtCache := range stats.Resctrl.Cache {
		tags := map[string]string{
			"node_id": fmt.Sprintf("%v", nodeID),
		}
		point := makePoint(serResctrlLLCOccupancy, rdtCache.LLCOccupancy)
		addTagsToPoint(point, tags)
		points = append(points, point)
	}

	s.tagPoints(cInfo, stats, points)

	return points
}

func (s *influxdbStorage2) OverrideReadyToFlush(readyToFlush func() bool) {
	s.readyToFlush = readyToFlush
}

func (s *influxdbStorage2) defaultReadyToFlush() bool {
	return time.Since(s.lastWrite) >= s.bufferDuration
}

func (s *influxdbStorage2) AddStats(cInfo *info.ContainerInfo, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}
	var pointsToFlush []*write.Point
	func() {
		// AddStats will be invoked simultaneously from multiple threads and only one of them will perform a write.
		s.lock.Lock()
		defer s.lock.Unlock()

		s.points = append(s.points, s.containerStatsToPoints(cInfo, stats)...)
		s.points = append(s.points, s.memoryStatsToPoints(cInfo, stats)...)
		s.points = append(s.points, s.hugetlbStatsToPoints(cInfo, stats)...)
		s.points = append(s.points, s.perfStatsToPoints(cInfo, stats)...)
		s.points = append(s.points, s.resctrlStatsToPoints(cInfo, stats)...)
		s.points = append(s.points, s.containerFilesystemStatsToPoints(cInfo, stats)...)
		if s.readyToFlush() {
			pointsToFlush = s.points
			s.points = make([]*write.Point, 0)
			s.lastWrite = time.Now()
		}
	}()
	if len(pointsToFlush) > 0 {
		points := make([]*write.Point, len(pointsToFlush))
		for i, p := range pointsToFlush {
			p.AddTag(tagMachineName, "Machine Name")
			p.AddField(tagMachineName, s.machineName)
			points[i] = p
		}

		err := s.WriteAPIBlocking.WritePoint(context.Background(), points...)
		if err != nil || checkResponseForErrors(err) != nil {
			return fmt.Errorf("failed to write stats to influxDb - %s", err)
		}

	}
	return nil
}

func (s *influxdbStorage2) Close() error {
	if s.client != nil {
		s.client.Close()
	}

	s.client = nil
	return nil
}

// machineName: A unique identifier to identify the host that current cAdvisor
// instance is running on.
// influxdbHost: The host which runs influxdb (host:port)
func newStorage(
	machineName,
	buketName,
	orgName,
	authToken,
	influxdbHost string,
	isSecure bool,
	bufferDuration time.Duration,
) (*influxdbStorage2, error) {
	url := &url.URL{
		Scheme: "http",
		Host:   influxdbHost,
	}
	if isSecure {
		url.Scheme = "https"
	}
	cadvisorVersion := fmt.Sprintf("%v/%v", "cAdvisor", version.Info["version"])
	opts := influxdb2.DefaultOptions()
	opts.HTTPOptions().SetHTTPDoer(&UserAgentSetter{
		UserAgent:   cadvisorVersion + " FM-Container-Monitor/1.1 ",
		RequestDoer: http.DefaultClient,
	})

	opts.SetLogLevel(0).
		SetBatchSize(100)

	client := influxdb2.NewClientWithOptions(url.String(), authToken, opts)
	writeAPIBlocking := client.WriteAPIBlocking(orgName, buketName)
	writeAPI := client.WriteAPI(orgName, buketName)

	ret := &influxdbStorage2{
		client:           client,
		machineName:      machineName,
		orgName:          orgName,
		writeAPI:         writeAPI,
		WriteAPIBlocking: writeAPIBlocking,
		bufferDuration:   bufferDuration,
		lastWrite:        time.Now(),
		points:           make([]*write.Point, 0),
	}
	ret.readyToFlush = ret.defaultReadyToFlush
	return ret, nil
}

// Creates a measurement point with a single value field
func makePoint(name string, value interface{}) *write.Point {
	return influxdb2.NewPoint(name,
		map[string]string{name: fieldType}, //TODO 根据不同计量名字，给出对应的计量单位
		map[string]interface{}{name: value},
		time.Now())
}

// Adds additional tags to the existing tags of a point
func addTagsToPoint(point *write.Point, tags map[string]string) {
	for k, v := range tags {
		point.AddTag(k, v)
	}
}

// Checks response for possible errors
func checkResponseForErrors(err error) error {
	const msg = "failed to write stats to influxDb - %s"

	if err != nil {
		return fmt.Errorf(msg, err.Error())
	}

	return nil
}

// convertField converts any primitive type to types supported by line protocol
func convertField(v interface{}) interface{} {
	switch v := v.(type) {
	case bool, int64, string, float64:
		return v
	case int:
		return int64(v)
	case uint:
		return uint64(v)
	case uint64:
		return v
	case []byte:
		return string(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint32:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint8:
		return uint64(v)
	case float32:
		return float64(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case time.Duration:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
