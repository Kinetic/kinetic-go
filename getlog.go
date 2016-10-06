package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type LogType int32

const (
	_                 LogType = iota
	LOG_UTILIZATIONS  LogType = iota
	LOG_TEMPERATURES  LogType = iota
	LOG_CAPACITIES    LogType = iota
	LOG_CONFIGURATION LogType = iota
	LOG_STATISTICS    LogType = iota
	LOG_MESSAGES      LogType = iota
	LOG_LIMITS        LogType = iota
	LOG_DEVICE        LogType = iota
)

var strLogType = map[LogType]string{
	LOG_UTILIZATIONS:  "LOG_UTILIZATIONS",
	LOG_TEMPERATURES:  "LOG_TEMPERATURES",
	LOG_CAPACITIES:    "LOG_CAPACITIES",
	LOG_CONFIGURATION: "LOG_CONFIGURATION",
	LOG_STATISTICS:    "LOG_STATISTICS",
	LOG_MESSAGES:      "LOG_MESSAGES",
	LOG_LIMITS:        "LOG_LIMITS",
	LOG_DEVICE:        "LOG_DEVICE",
}

func (l LogType) String() string {
	str, ok := strLogType[l]
	if ok {
		return str
	}
	return "Unknown LogType"
}

func convertLogTypeToProto(l LogType) kproto.Command_GetLog_Type {
	ret := kproto.Command_GetLog_INVALID_TYPE
	switch l {
	case LOG_UTILIZATIONS:
		ret = kproto.Command_GetLog_UTILIZATIONS
	case LOG_TEMPERATURES:
		ret = kproto.Command_GetLog_TEMPERATURES
	case LOG_CAPACITIES:
		ret = kproto.Command_GetLog_CAPACITIES
	case LOG_CONFIGURATION:
		ret = kproto.Command_GetLog_CONFIGURATION
	case LOG_STATISTICS:
		ret = kproto.Command_GetLog_STATISTICS
	case LOG_MESSAGES:
		ret = kproto.Command_GetLog_MESSAGES
	case LOG_LIMITS:
		ret = kproto.Command_GetLog_LIMITS
	case LOG_DEVICE:
		ret = kproto.Command_GetLog_DEVICE
	}
	return ret
}

func convertLogTypeFromProto(l kproto.Command_GetLog_Type) LogType {
	var ret LogType
	switch l {
	case kproto.Command_GetLog_UTILIZATIONS:
		ret = LOG_UTILIZATIONS
	case kproto.Command_GetLog_TEMPERATURES:
		ret = LOG_TEMPERATURES
	case kproto.Command_GetLog_CAPACITIES:
		ret = LOG_CAPACITIES
	case kproto.Command_GetLog_CONFIGURATION:
		ret = LOG_CONFIGURATION
	case kproto.Command_GetLog_STATISTICS:
		ret = LOG_STATISTICS
	case kproto.Command_GetLog_MESSAGES:
		ret = LOG_MESSAGES
	case kproto.Command_GetLog_LIMITS:
		ret = LOG_LIMITS
	case kproto.Command_GetLog_DEVICE:
		ret = LOG_DEVICE
	}
	return ret
}

// UtilizationLog for kinetic device utilization information.
type UtilizationLog struct {
	Name  string  // Name of the device utlity
	Value float32 // Value of device utility
}

// TemperatureLog for kinetic device tempture.
type TemperatureLog struct {
	Name    string  // Name of the device
	Current float32 // Current Temperature
	Minimum float32 // Minimum Temperature for drive
	Maximum float32 // Maximum Tempture for drive
	Target  float32 // Target Temperature for drive
}

// CapacityLog for kinetic device capacity information.
type CapacityLog struct {
	CapacityInBytes uint64  // total capacity of hard disk, in bytes
	PortionFull     float32 // remaining capacity of hard disk
}

// ConfigurationInterface for kinetic device network interfaces information.
type ConfigurationInterface struct {
	Name     string // network device name
	MAC      []byte // network device mac address
	Ipv4Addr []byte // network device ipv4 address
	Ipv6Addr []byte // network device ipv6 address
}

// ConfigurationLog for kinetic device configuration information.
type ConfigurationLog struct {
	Vendor                  string                   // Vendor name
	Model                   string                   // Device model
	SerialNumber            []byte                   // Device serial number
	WorldWideName           []byte                   // Device world wide name
	Version                 string                   // Device version
	CompilationDate         string                   // Device service code compilation date
	SourceHash              string                   // Device service source code repository hash value
	ProtocolVersion         string                   // Device supported protocol version
	ProtocolCompilationDate string                   // Device supported protocol compilation date
	ProtocolSourceHash      string                   // Device supported protocol source code repository hash value
	Interface               []ConfigurationInterface // Device interfaces as list
	Port                    int32                    // Service port
	TlsPort                 int32                    // TLS service port
}

// StatisticsLog information for each type of MessageType.
// Count is total number of Type message processed.
// Bytes is the sum of the data that is in the data portion.
// This does not include the command description.
// For P2P operations, this is the amount of data moved between drives
type StatisticsLog struct {
	// TODO: Would it better just use the protocol Command_MessageType?
	Type  MessageType
	Count uint64
	Bytes uint64
}

// LimitsLog defines max values.
type LimitsLog struct {
	MaxKeySize                  uint32 // max key size
	MaxValueSize                uint32 // max value size
	MaxVersionSize              uint32 // max version size
	MaxTagSize                  uint32 // max tag size
	MaxConnections              uint32 // max connection
	MaxOutstandingReadRequests  uint32 // max out standing read request
	MaxOutstandingWriteRequests uint32 // max out standing write request
	MaxMessageSize              uint32 // max message size
	MaxKeyRangeCount            uint32 // max key range count
	MaxIdentityCount            uint32 // max identity count
	MaxPinSize                  uint32 //
	MaxOperationCountPerBatch   uint32 //
	MaxBatchCountPerDevice      uint32 //
}

type DeviceLog struct {
	Name []byte
}

// Log is the top level structure that groups all the log information
type Log struct {
	Utilizations  []UtilizationLog  // List of utilization information of the drive
	Temperatures  []TemperatureLog  // List of tempeture inforamtion of the drive
	Capacity      *CapacityLog      // Capacity information of the drive
	Configuration *ConfigurationLog // Configuration information of the drive
	Statistics    []StatisticsLog   // List of statistic information from the drive
	Messages      []byte            // Kinetic log messages from the drive
	Limits        *LimitsLog        // Limits information from the drive
	Device        *DeviceLog
}

func getUtilizationLogFromProto(getlog *kproto.Command_GetLog) (log []UtilizationLog) {
	log = nil
	utils := getlog.GetUtilizations()
	if utils != nil {
		log = make([]UtilizationLog, len(utils))
		for k, v := range utils {
			log[k] = UtilizationLog{
				Name:  v.GetName(),
				Value: v.GetValue(),
			}
		}
	}
	return
}

func getTemperatureLogFromProto(getlog *kproto.Command_GetLog) (log []TemperatureLog) {
	log = nil
	temps := getlog.GetTemperatures()
	if temps != nil {
		log = make([]TemperatureLog, len(temps))
		for k, v := range temps {
			log[k] = TemperatureLog{
				Name:    v.GetName(),
				Current: v.GetCurrent(),
				Minimum: v.GetMinimum(),
				Maximum: v.GetMaximum(),
				Target:  v.GetTarget(),
			}
		}
	}
	return
}

func getCapacityLogFromProto(getlog *kproto.Command_GetLog) (log *CapacityLog) {
	log = nil
	capacity := getlog.GetCapacity()
	if capacity != nil {
		log = &CapacityLog{
			CapacityInBytes: capacity.GetNominalCapacityInBytes(),
			PortionFull:     capacity.GetPortionFull(),
		}
	}
	return
}

func getConfigurationInterfaceFromProto(conf *kproto.Command_GetLog_Configuration) (inf []ConfigurationInterface) {
	inf = nil
	pinf := conf.GetInterface()
	if pinf != nil {
		inf = make([]ConfigurationInterface, len(pinf))
		for k, v := range pinf {
			inf[k] = ConfigurationInterface{
				Name:     v.GetName(),
				MAC:      v.GetMAC(),
				Ipv4Addr: v.GetIpv4Address(),
				Ipv6Addr: v.GetIpv6Address(),
			}
		}
	}
	return
}

func getConfigurationLogFromProto(getlog *kproto.Command_GetLog) (log *ConfigurationLog) {
	log = nil
	conf := getlog.GetConfiguration()
	if conf != nil {
		log = &ConfigurationLog{
			Vendor:                  conf.GetVendor(),
			Model:                   conf.GetModel(),
			SerialNumber:            conf.GetSerialNumber(),
			WorldWideName:           conf.GetWorldWideName(),
			Version:                 conf.GetVersion(),
			CompilationDate:         conf.GetCompilationDate(),
			SourceHash:              conf.GetSourceHash(),
			ProtocolVersion:         conf.GetProtocolVersion(),
			ProtocolCompilationDate: conf.GetProtocolCompilationDate(),
			ProtocolSourceHash:      conf.GetProtocolSourceHash(),
			Interface:               getConfigurationInterfaceFromProto(conf),
			Port:                    conf.GetPort(),
			TlsPort:                 conf.GetTlsPort(),
		}
	}
	return
}

func getStatisticsLogFromProto(getlog *kproto.Command_GetLog) (log []StatisticsLog) {
	log = nil
	statics := getlog.GetStatistics()
	if statics != nil {
		log := make([]StatisticsLog, len(statics))
		for k, v := range statics {
			log[k] = StatisticsLog{
				Type:  convertMessageTypeFromProto(v.GetMessageType()),
				Count: v.GetCount(),
				Bytes: v.GetBytes(),
			}
		}
	}
	return
}

func getLogMessageFromProto(getlog *kproto.Command_GetLog) []byte {
	return getlog.GetMessages()
}

func getLimitsLogFromProto(getlog *kproto.Command_GetLog) (log *LimitsLog) {
	log = nil
	limits := getlog.GetLimits()
	if limits != nil {
		log = &LimitsLog{
			MaxKeySize:                  limits.GetMaxKeySize(),
			MaxValueSize:                limits.GetMaxValueSize(),
			MaxVersionSize:              limits.GetMaxVersionSize(),
			MaxTagSize:                  limits.GetMaxTagSize(),
			MaxConnections:              limits.GetMaxConnections(),
			MaxOutstandingReadRequests:  limits.GetMaxOutstandingReadRequests(),
			MaxOutstandingWriteRequests: limits.GetMaxOutstandingWriteRequests(),
			MaxMessageSize:              limits.GetMaxMessageSize(),
			MaxKeyRangeCount:            limits.GetMaxKeyRangeCount(),
			MaxIdentityCount:            limits.GetMaxIdentityCount(),
			MaxPinSize:                  limits.GetMaxPinSize(),
			MaxOperationCountPerBatch:   limits.GetMaxOperationCountPerBatch(),
			MaxBatchCountPerDevice:      limits.GetMaxBatchCountPerDevice(),
		}
	}
	return
}

func getDeviceLogFromProto(getlog *kproto.Command_GetLog) *DeviceLog {
	//TODO: Need more details
	return &DeviceLog{
		Name: getlog.GetDevice().GetName(),
	}
}

func getLogFromProto(resp *kproto.Command) Log {
	var logs Log

	getlog := resp.GetBody().GetGetLog()

	if getlog != nil {
		logs = Log{
			Utilizations:  getUtilizationLogFromProto(getlog),
			Temperatures:  getTemperatureLogFromProto(getlog),
			Capacity:      getCapacityLogFromProto(getlog),
			Configuration: getConfigurationLogFromProto(getlog),
			Statistics:    getStatisticsLogFromProto(getlog),
			Messages:      getLogMessageFromProto(getlog),
			Limits:        getLimitsLogFromProto(getlog),
			Device:        getDeviceLogFromProto(getlog),
		}
	}
	return logs
}
