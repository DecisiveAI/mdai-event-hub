package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	v1 "github.com/decisiveai/mdai-operator/api/v1"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ConfigMapManager manages multiple ConfigMap fetchers
type ConfigMapManager struct {
	clientset    *kubernetes.Clientset
	namespace    string
	logger       *log.Logger
	fetchers     map[string]*ConfigMapFetcher
	fetchersLock sync.RWMutex
	suffix       string
}

// ConfigMapFetcher provides methods to fetch and watch a specific ConfigMap
type ConfigMapFetcher struct {
	clientset     *kubernetes.Clientset
	namespace     string
	configMapName string
	data          map[string]string
	dataLock      sync.RWMutex
	logger        *log.Logger
	lastUpdated   time.Time
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewConfigMapManager creates a new ConfigMapManager
func NewConfigMapManager(suffix string) (*ConfigMapManager, error) {
	// Create in-cluster config
	//config, err := rest.InClusterConfig()
	//if err != nil {
	//	return nil, fmt.Errorf("failed to create in-cluster config: %v", err)
	//}

	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig, err := os.UserHomeDir()
		if err != nil {
			logger.Error("Failed to load k8s config", zap.Error(err))
			return nil, err
		}

		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig+"/.kube/config")
		if err != nil {
			logger.Error("Failed to build k8s config", zap.Error(err))
			return nil, err
		}
	}

	// Create clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}

	// Get namespace from the pod's environment or use default
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		// Try to get namespace from the service account token
		data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err == nil {
			namespace = string(data)
		} else {
			namespace = "default" // Fallback to default namespace
		}
	}

	return &ConfigMapManager{
		clientset: clientset,
		namespace: namespace,
		logger:    log.New(os.Stdout, "[ConfigMapManager] ", log.LstdFlags),
		fetchers:  make(map[string]*ConfigMapFetcher),
		suffix:    suffix,
	}, nil
}

// GetConfigMapForHub returns the ConfigMap data for a specific hub
// It creates a new fetcher if one doesn't exist for this hub
func (m *ConfigMapManager) GetConfigMapForHub(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error) {
	configMapName := hubName + m.suffix

	// Check if we already have a fetcher for this hub
	m.fetchersLock.RLock()
	fetcher, exists := m.fetchers[hubName]
	m.fetchersLock.RUnlock()

	if !exists {
		// Create a new fetcher for this hub
		m.logger.Printf("Creating new fetcher for hub %s (ConfigMap: %s)", hubName, configMapName)

		fetcherCtx, cancel := context.WithCancel(context.Background())
		fetcher = &ConfigMapFetcher{
			clientset:     m.clientset,
			namespace:     m.namespace,
			configMapName: configMapName,
			data:          make(map[string]string),
			logger:        log.New(os.Stdout, fmt.Sprintf("[ConfigMapFetcher:%s] ", hubName), log.LstdFlags),
			ctx:           fetcherCtx,
			cancel:        cancel,
		}

		// Start watching for changes to this ConfigMap
		go fetcher.watchConfigMap()

		// Store the fetcher
		m.fetchersLock.Lock()
		m.fetchers[hubName] = fetcher
		m.fetchersLock.Unlock()

		// Initial fetch (blocking to ensure we have data before returning)
		err := fetcher.fetchConfigMap(ctx)
		if err != nil {
			m.logger.Printf("Warning: Initial fetch for hub %s failed: %v", hubName, err)
			// We don't return error here, as the watch might succeed later
		}
	}

	// Get current data
	fetcher.dataLock.RLock()
	defer fetcher.dataLock.RUnlock()

	// Return a copy to prevent race conditions
	result := make(map[string][]v1.AutomationStep, len(fetcher.data))

	for k, v := range fetcher.data {
		var workflow []v1.AutomationStep

		if err := json.Unmarshal([]byte(v), &workflow); err != nil {
			m.logger.Printf("could not unmarshall workflow %s: %v", k, err)
			continue
		}

		result[k] = workflow
	}

	return result, nil
}

// Cleanup stops all watchers and releases resources
func (m *ConfigMapManager) Cleanup() {
	m.fetchersLock.Lock()
	defer m.fetchersLock.Unlock()

	for hubName, fetcher := range m.fetchers {
		m.logger.Printf("Stopping watcher for hub %s", hubName)
		fetcher.cancel()
	}
}

// RemoveHubFetcher stops and removes a hub's fetcher
func (m *ConfigMapManager) RemoveHubFetcher(hubName string) {
	m.fetchersLock.Lock()
	defer m.fetchersLock.Unlock()

	if fetcher, exists := m.fetchers[hubName]; exists {
		m.logger.Printf("Removing fetcher for hub %s", hubName)
		fetcher.cancel()
		delete(m.fetchers, hubName)
	}
}

// watchConfigMap continuously watches for changes to a ConfigMap
func (f *ConfigMapFetcher) watchConfigMap() {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-f.ctx.Done():
			f.logger.Printf("Watcher for ConfigMap %s stopped", f.configMapName)
			return
		default:
			// Continue with watch setup
		}

		// Setup watcher
		f.logger.Printf("Setting up watcher for ConfigMap %s", f.configMapName)
		watcher, err := f.clientset.CoreV1().ConfigMaps(f.namespace).Watch(f.ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", f.configMapName),
		})

		if err != nil {
			f.logger.Printf("Failed to watch ConfigMap %s: %v. Retrying in %v...",
				f.configMapName, err, backoff)

			select {
			case <-time.After(backoff):
				// Increase backoff for next attempt
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			case <-f.ctx.Done():
				return
			}
			continue
		}

		// Reset backoff on successful watch setup
		backoff = 1 * time.Second

		f.logger.Printf("Successfully set up watcher for ConfigMap %s", f.configMapName)

		// Process events
		for event := range watcher.ResultChan() {
			if f.ctx.Err() != nil {
				watcher.Stop()
				return
			}

			switch event.Type {
			case "ADDED", "MODIFIED":
				configMap, ok := event.Object.(*corev1.ConfigMap)
				if !ok {
					f.logger.Printf("Unexpected object type: %T", event.Object)
					continue
				}

				f.dataLock.Lock()
				f.data = configMap.Data
				f.lastUpdated = time.Now()
				f.dataLock.Unlock()

				f.logger.Printf("ConfigMap %s was %s, updated with %d entries",
					f.configMapName, event.Type, len(configMap.Data))

			case "DELETED":
				f.logger.Printf("ConfigMap %s was deleted", f.configMapName)
				// Keep the last known data but mark it as potentially stale
			}
		}

		f.logger.Printf("Watcher for ConfigMap %s ended, will reconnect", f.configMapName)
	}
}

// fetchConfigMap gets the current state of the ConfigMap
func (f *ConfigMapFetcher) fetchConfigMap(ctx context.Context) error {
	configMap, err := f.clientset.CoreV1().ConfigMaps(f.namespace).Get(ctx, f.configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get ConfigMap %s: %v", f.configMapName, err)
	}

	f.dataLock.Lock()
	f.data = configMap.Data
	f.lastUpdated = time.Now()
	f.dataLock.Unlock()

	f.logger.Printf("Successfully fetched ConfigMap %s with %d entries", f.configMapName, len(configMap.Data))
	return nil
}
