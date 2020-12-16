package resource

import (
	"context"
	"fmt"
	"github.com/k0kubun/pp"
	"github.com/panjf2000/ants/v2"
	sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/metrics"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/worker"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools/unit"
	client "gitlab.mfwdev.com/servicemesh/robot"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// K8S provider implement
type k8s struct {
	robot      client.Robot     // the K8s multi-cluster aggregator
	ctx        context.Context  // context
	worker     worker.Worker    // the role of worker is to synchronize provider changes to the discovery center
	stopped    bool             // whether the current provider has stopped
	sync.Mutex                  // lock mutex
	interval   int              // the time interval for full synchronization. default 600s(10m)
	filters    []InstanceFilter // finters is a collection of functions used to filter invalid instances
	cache      *Cache           // pod cache
	pool	   *ants.Pool		// goroutine pool
}

const (
	InstanceOnline   = "online"
	InstanceOffline  = "offline"
	InstancePrepared = "prepared"
	InstanceFailed   = "failed"
	InstanceStatus   = 1 // 0下线 1上线 -1全量
)

const (
	ProtoHTTP      = "http"
	ProtoGRPC      = "grpc"
	ProtoWebSocket = "websocket"
	ProtoDubbo     = "dubbo"
	PoolBenchSize  = 100
	PoolExpireTime = 100
)

type RuntimeConfig struct {
	Cpu          float32           `json:"cpu"`    // cpu size
	Memory       int32             `json:"memory"` // memory size
	Image        map[string]string `json:"image"`
	Environments map[string]string `json:"environments"`
}

type InstanceFilter func(ins *sv.Instance) bool

// WithExpiryDuration sets up the interval time of cleaning up goroutines.
func WithExpiryDuration(expiryDuration time.Duration) ants.Option {
	return func(opts *ants.Options) {
		opts.ExpiryDuration = expiryDuration
	}
}

// Init k8s provider
func NewK8SProvider(ctx context.Context, worker worker.Worker, pushInterval int, configPath []string) (provider K8SProvider, err error) {
	clusters := make([]client.Cluster, len(configPath))
	for idx, path := range configPath {
		clusters[idx] = client.Cluster{
			ConfigPath: path,
			Resources: []client.RN{
				{
					client.Pods,
					"",
				},
			},
		}
	}
	var kr client.Robot
	kr, err = client.NewRobot(clusters...)
	if err != nil {
		// If walk here, server init failed
		return nil, err
	}

	// init k8s obj
	k := &k8s{
		robot:    kr,
		ctx:      ctx,
		worker:   worker,
		interval: pushInterval,
		cache:    NewCache(2),
	}
	p, _ := ants.NewPool(PoolBenchSize, WithExpiryDuration(time.Second * PoolExpireTime))
	k.pool = p
	k.initInstanceFilters()

	provider = k

	return
}

// Monitor k8s cluster pod changes
func (k *k8s) Start() {
	go k.robot.Run()
	// TODO make sure all task done
	time.Sleep(time.Second * 5)
	k.monitor()

	log.Logger.Info("k8s resource worker stopped")
}

// monitor k8s pod changes
func (k *k8s) monitor() {
	// first we should flush all the instances
	// k.flushInstances()
	// synchronize periodically
	go k.processIntervalFullPush()
	go k.compareAndFlush()
	// monitor instance changes
	for {
		select {
		case <-k.ctx.Done():
			k.stopped = true
			k.robot.Stop()
			goto monitorEnd
		default:
			// get pod changes from k8s client
			obj, err := k.robot.Pop()
			if err != nil {
				log.Logger.Error("K8S watch error: ", err)
				time.Sleep(1 * time.Second)
				continue
			}
			// trigger time
			triggerTime := obj.CreateAt.Unix()
			// instance format
			var ins *sv.Instance
			if ins = k.convertK8sPod2Instance(obj); ins == nil {
				continue
			}
			// rsync
			go k.eventSync(ins, triggerTime)
			k.robot.Finish(obj)
		}
	}

monitorEnd:
	log.Logger.Info("exit the k8s monitor")
}

//
func (k *k8s) ProcessCache(obj client.QueueObject, ins *sv.Instance) {
	switch obj.Event {
	case client.EventAdd:
		k.cache.ReplaceOrInsert(ins)
	case client.EventUpdate:
		k.cache.ReplaceOrInsert(ins)
	case client.EventDelete:
		k.cache.Delete(ins.InstanceId)
	}
}

func (k *k8s) initInstanceFilters() {
	k.filters = []InstanceFilter{}
	// init a default instance filter
	k.filters = append(k.filters, func(ins *sv.Instance) bool {
		if ins == nil {
			return false
		}
		// appcode
		if ins.AppCode == "" {
			return false
		}
		// env_type
		if ins.EnvType == "" {
			return false
		}
		// ...
		return true
	})
}

// VerifyInstance checks wether the instance is valid
func (k *k8s) VerifyInstance(ins *sv.Instance) bool {
	if k.filters != nil && len(k.filters) > 0 {
		for _, f := range k.filters {
			if !f(ins) {
				return false
			}
		}
	}

	return true
}

// eventAdd get pod info by key, find an instance from mysql where health_state != Running
// and update this instance by updateInstance
func (k *k8s) eventSync(ins *sv.Instance, triggerTime int64) {
	k.worker.Handle(&worker.Event{
		Trigger: triggerTime,
		Data:    []*sv.Instance{ins},
		Operate: worker.OperateTypeSync,
	})
}

func (k *k8s) convertK8sPod2Instance(obj client.QueueObject) (ins *sv.Instance) {
	// get pod info from k8s robot
	items, ok := k.robot.GetByKey(client.Pods, obj.Key)
	if ok && len(items) > 0 {
		pod := items[0].(*v1.Pod)
		instance := formatInstance(&obj, pod)
		// if instance status is 0 or cache is nil and cache status not equals current status ,send sync event, otherwise don't repeat send
		if instance.Status == 0 {
			return nil
		}
		cacheInstance := k.cache.Get(instance.InstanceId)
		if cacheInstance == nil || cacheInstance.Status != instance.Status {
			// put all exist instance to cache, purpose for get cache don't make npe
			k.ProcessCache(obj, instance)
			if k.VerifyInstance(instance) {
				ins = instance
			} else {
				log.Logger.Warnf("invalid instance: %s", instance.InstanceId)
			}
		}
	} else {
		// If we cannot get the instance data, it means that this may be a DELETE event. At this point, the data in the
		// K8s robot no longer exists. However, in this scenario, the consumer of the instance needs
		// not only status = 2 but also the complete field data of the instance. Therefore, we have to fetch it from the cache.
		switch obj.Event {
		case client.EventAdd:
			fallthrough
			// log error
		case client.EventUpdate:
			// log error
		case client.EventDelete:
			instanceId := k.formatObjToInstanceId(obj)
			pp.Println(fmt.Sprintf("delete event, instanceid: %s", instanceId))
			if instance := k.cache.Get(instanceId); instance != nil {
				if instance.Status != 2 {
					// set instance status
					instance.Status = 2
					// delete cache
					k.cache.Delete(instance.InstanceId)
					ins = instance
				}
			}
		}
	}

	return ins
}

func (k *k8s) formatObjToInstanceId(obj client.QueueObject) string {
	if obj.Key != "" {
		keys := strings.Split(obj.Key, "/")
		if keys != nil && len(keys) >= 2 {
			return keys[1]
		}
	}

	return ""
}

// flush all instances
func (k *k8s) flushInstances() {
	if all := k.GetAll(); all != nil && len(all) > 0 {
		// flush the original cache and fill it
		before := time.Now()
		k.cache.Clear()
		for _, ins := range all {
			k.cache.ReplaceOrInsert(ins)
		}
		log.Logger.Infof("flush k8s cache spend time: %s", unit.RelTime(before, time.Now(), "", ""))
		// push all
		event := &worker.Event{
			Trigger: time.Now().Unix(),
			Data:    all,
			Operate: worker.OperateTypeSyncAll}
		k.worker.Handle(event)
	}
}

// compare and find diff instances then flush
func (k *k8s) compareAndFlush() {
	if all := k.GetAll(); all != nil && len(all) > 0 {
		list, err := k.worker.GetAll(InstanceStatus)
		if err != nil || list == nil || len(list.GetInstance()) <= 0 {
			log.Logger.Errorf("get all instances from atlas failed")
			return
		}
		servMap := k.listToMap(list.GetInstance())
		k8sMap := k.listToMap(all)
		log.Logger.Infof("atlas server instance size :%d  k8s instance size :%d", len(servMap), len(k8sMap))
		for k8sKey, k8sIns := range k8sMap {
			if servIns, exist := servMap[k8sKey]; exist {
				if k8sIns.Reversion > servIns.Reversion {
					k.buildAndSendEvent(k8sIns)
				}
				delete(k8sMap, k8sKey)
				delete(servMap, k8sKey)
			} else {
				log.Logger.Infof("k8s match much : %v \n", k8sIns.InstanceId)
				if k8sIns.Status == 1 {
					k.buildAndSendEvent(k8sIns)
				}
				delete(k8sMap, k8sKey)
			}
		}
		if len(servMap) > 0 {
			log.Logger.Infof("atlas server pre delete instance size :%d \n", len(servMap))
			for _, servIns := range servMap {
				servIns.Status = 2
				k.buildAndSendEvent(servIns)
			}
		}
	}
}

func (k *k8s) buildAndSendEvent(instance *sv.Instance) {
	// if instance status is 0 or cached instance exist and status equals current status, don't send event
	k.pool.Submit(func() {
		if instance.Status == 0 {
			return
		}
		cacheInstance := k.cache.Get(instance.InstanceId)
		if cacheInstance != nil && cacheInstance.Status == instance.Status {
			return
		}
		ins := make([]*sv.Instance, 1)
		ins[0] = instance
		triggerTime := time.Now().Unix()
		event := &worker.Event{
			Trigger: triggerTime,
			Data:    ins,
			Operate: worker.OperateTypeSync,
		}
		k.worker.Handle(event)
	})
}

func (k *k8s) listToMap(ins []*sv.Instance) (m map[string]*sv.Instance) {
	m = make(map[string]*sv.Instance)
	for _, value := range ins {
		m[value.InstanceId] = value
	}
	return
}

func (k *k8s) GetAll() (result []*sv.Instance) {
	items := k.robot.List(client.Pods)
	for _, item := range items {
		pod := item.(*v1.Pod)
		instance := formatInstance(nil, pod)
		if k.VerifyInstance(instance) {
			if result == nil {
				result = []*sv.Instance{}
			}
			result = append(result, instance)
		} else {
			log.Logger.Warnf("invalid instance: %s", instance.InstanceId)
		}
	}
	log.Logger.Infof("k8s getAll size: %d \n", len(result))
	return
}

func (k *k8s) processIntervalFullPush() {
	interval := 7200
	if k.interval != 0 {
		interval = k.interval
	}
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		select {
		case <-ticker.C:
			before := time.Now()
			k.compareAndFlush()
			after := time.Now()
			offset := after.Sub(before).Milliseconds()
			metrics.SyncAllDurationsHistogram.Observe(float64(offset))
			log.Logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
		case <-k.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// TODO obj param optimize
func formatInstance(obj *client.QueueObject, pod *v1.Pod) (ins *sv.Instance) {
	if pod == nil {
		return ins
	}
	// labels
	labels := pod.Labels
	var runtimeConfig *RuntimeConfig
	var envType string
	var ports []*sv.PortInfo
	var cpu float32
	var memory int32
	var envs map[string]string
	var images = make(map[string]string)
	// A pod may contains more than one containers
	for _, container := range pod.Spec.Containers {

		// only get the  container env info that named "application"
		if container.Name == "application" {
			envs = make(map[string]string, len(container.Env))
			for _, env := range container.Env {
				envs[env.Name] = env.Value
			}

		}
		images[container.Name] = container.Image
		for _, e := range container.Env {
			if e.Name == "K8S_CLUSTER_TYPE" {
				envType = e.Value
			}
		}
		// merge containers cpu and memory limit value
		cpu = cpu + formatCpuSize(container.Resources.Limits.Cpu())
		memory = memory + formatMemorySize(container.Resources.Limits.Memory())
	}
	// format container port
	ports = formatAppPort(pod)

	// cpu and memroy
	runtimeConfig = &RuntimeConfig{
		Cpu:          cpu,
		Memory:       memory,
		Image:        images,
		Environments: envs,
	}
	// format state and appcode
	state := formatState(pod)
	appCode := formatAppCode(pod)
	status := formatInstanceStatus(obj, pod)
	// filter the invalid pod
	if appCode == "" {
		return
	}
	envType = formatEnvType(pod, envType)
	// enable state
	enabled := formatContainerEnabled(pod)
	// reversion
	// pay attention. The reversion field is the resource version of the instance, which is strictly self-increasing.
	// The push behavior of data may not be able to reach the opposite end in an orderly manner under a complex network environment.
	// Therefore, when the opposite end receives data, it needs to compare the reversion value with the existing value.
	// If the value is larger, it can be stored in the database. Otherwise, refuse to update.
	reversion, _ := strconv.ParseInt(pod.ObjectMeta.ResourceVersion, 10, 64)

	// envgroup
	envGroup := ""
	if eg, ok := labels["env-group"]; ok && eg != "" {
		envGroup = eg
	}

	// envCode
	envCode := envType + "#" + envGroup

	// set application.name
	label := formatLableInfo(pod, labels, runtimeConfig.Environments)

	// convert pod to instance
	ins = &sv.Instance{
		InstanceId:  pod.Name,
		Ports:       ports,
		AppCode:     appCode,
		EnvCode:     envCode,
		EnvType:     envType,
		EnvGroup:    envGroup,
		Version:     labels["version"],
		HealthState: "passing",
		Ip:          pod.Status.PodIP,
		Enabled:     enabled,
		State:       state,
		Provider:    "k8s",
		Hostname:    pod.Name,
		Cpu:         runtimeConfig.Cpu,
		Memory:      runtimeConfig.Memory,
		Image:       runtimeConfig.Image,
		Idc:         labels["version"],
		Reversion:   reversion,
		Status:      status,
		Label:       label,
	}

	return
}

// format lable info
func formatLableInfo(pod *v1.Pod, originLables map[string]string, envs map[string]string) (lablel map[string]string) {
	if lablel == nil {
		lablel = make(map[string]string)
	}
	// for Java SDK
	if envs != nil && len(envs) > 0 {
		if san, exist := envs["spring.application.name"]; exist {
			lablel["env:san"] = san
		}
	}
	// for namespace
	lablel["compatibility:aos_namespace"] = ""
	if pod != nil {
		lablel["compatibility:aos_namespace"] = pod.Namespace
	}
	if originLables != nil && len(originLables) > 0 {
		// for specific label
		lablel["compatibility:aos_app"] = ""
		if lapp, exist := originLables["app"]; exist {
			lablel["compatibility:aos_app"] = lapp
		}
		// for destination rule and virtual service
		var drHost string
		// in case of FengXiao
		if _, exist := originLables["deploy-id"]; exist {
			drHost = lablel["compatibility:aos_app"] + "." + lablel["compatibility:aos_namespace"]
		} else {
			// in case of AosMicroservice
			drHost = lablel["compatibility:aos_app"]
		}
		lablel["compatibility:aos_dr_host"] = drHost
		// in case of WebIDE
		if mark, exist := originLables["mark"]; exist {
			lablel["compatibility:aos_mark"] = mark
		}
	}

	return
}

// formatAppPort format K8s container port to instance port info
func formatAppPort(pod *v1.Pod) (ports []*sv.PortInfo) {
	ports = []*sv.PortInfo{}
	if pod.Spec.Containers != nil && len(pod.Spec.Containers) > 0 {
		// TODO temp setting
		ports = append(ports, &sv.PortInfo{
			Name:     "dubbo" + "-" + strconv.FormatInt(7096, 10),
			Protocol: ProtoDubbo,
			Port:     7096,
		})
		for _, container := range pod.Spec.Containers {
			if container.Ports != nil && len(container.Ports) > 0 {
				for _, p := range container.Ports {
					proto := ""
					if p.Protocol == v1.ProtocolTCP {
						if strings.HasPrefix(p.Name, "http") {
							proto = ProtoHTTP
						} else if strings.HasPrefix(p.Name, "grpc") {
							proto = ProtoGRPC
						}
					}
					ports = append(ports, &sv.PortInfo{
						Name:     p.Name,
						Protocol: proto,
						Port:     p.ContainerPort,
					})

				}
			}
		}
	}

	return
}

func formatAppCode(pod *v1.Pod) (appCode string) {
	labels := pod.Labels
	if code, ok := labels["app-code"]; ok {
		appCode = code
	} else if code, ok := labels["cadvisor-app"]; ok {
		appCode = code
	} else {
		appCode = pod.Namespace + "-" + labels["name"]
	}
	return
}

// formatInstanceStatus convert instance status
func formatInstanceStatus(obj *client.QueueObject, pod *v1.Pod) (status int32) {
	if obj != nil && obj.Event == client.EventDelete {
		status = 2
	} else {
		if pod.DeletionTimestamp != nil {
			// modify 3 to 2
			status = 2
		} else if pod != nil && pod.Status.Phase == v1.PodRunning {
			var ready = true
			for _, c := range pod.Status.ContainerStatuses {
				if c.Ready == false || c.State.Running == nil {
					ready = false
				}
			}
			if ready == true {
				status = 1
			}
		}
	}

	return
}

func formatEnvType(pod *v1.Pod, envType string) string {
	labels := pod.Labels
	if env, ok := labels["env-type"]; ok && envType == "" {
		envType = env
	} else if env, ok := labels["K8S_CLUSTER_TYPE"]; ok && envType == "" {
		envType = env
	}
	envType = strings.ToLower(envType)
	if envType == "online" {
		envType = "product"
	}
	return envType
}

func formatContainerEnabled(pod *v1.Pod) (enabled bool) {
	if pod != nil && pod.Status.Phase == v1.PodRunning {
		var ready = true
		for _, c := range pod.Status.ContainerStatuses {
			if c.Ready == false || c.State.Running == nil {
				ready = false
			}
		}
		if pod.DeletionTimestamp != nil {
			ready = false
		}
		if ready == true {
			enabled = true
		}
	}

	return
}

// 1000m = 1
func formatCpuSize(r *resource.Quantity) (count float32) {
	if cpuInt, ok := r.AsInt64(); !ok {
		cpuStr := r.String()
		reg := regexp.MustCompile(`\d+`)
		c, err := strconv.Atoi(string(reg.Find([]byte(cpuStr))))
		if err != nil {
			log.Logger.Error("format cpu error: cpu=", cpuStr)
		}
		count = float32(c) / 1000
	} else {
		count = float32(cpuInt)
	}
	return
}

// formatMemorySize is responsible for formatting the memory value of the instance
func formatMemorySize(r *resource.Quantity) (memory int32) {
	memoryInt, _ := r.AsInt64()
	memoryStr := r.String()
	if strings.Contains(memoryStr, "i") {
		memory = int32(memoryInt / 1024 / 1024)
	} else {
		memory = int32(memoryInt / 1000 / 1000)
	}
	return
}

// formatState is responsible for formatting the state of the instance
func formatState(pod *v1.Pod) (state string) {

	if formatContainerEnabled(pod) {
		state = InstanceOnline
	} else {
		switch pod.Status.Phase {
		case v1.PodPending:
			state = InstancePrepared
		case v1.PodUnknown:
		case v1.PodFailed:
			state = InstanceFailed
		}
	}
	return
}
