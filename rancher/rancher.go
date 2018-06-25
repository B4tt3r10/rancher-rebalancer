package rancher

import (
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/imdario/mergo"
	rancher "github.com/rancher/go-rancher/v2"
)

type Client struct {
	client *rancher.RancherClient
}

func baseListOpts() *rancher.ListOpts {
	return &rancher.ListOpts{
		Filters: map[string]interface{}{
			"limit":  -2,
			"all":    true,
			"system": false,
		},
	}
}

func CreateClient(url, accessKey, secretKey string) *rancher.RancherClient {
	client, err := rancher.NewRancherClient(&rancher.ClientOpts{
		Url:       url,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Timeout:   time.Second * 5,
	})

	if err != nil {
		log.Errorf("failed to create a client for rancher, error: %s", err)
		os.Exit(1)
	}

	return client
}

// returns the project id of an environemnt
func GetProjectIdByName(client *rancher.RancherClient, name string) string {
	projects, err := client.Project.List(nil)
	if err != nil {
		log.Error("error getting project list")
	}

	for _, p := range projects.Data {
		if p.Name == name {
			return p.Id
		}
	}

	log.Error("project " + name + " not found")

	return "NotFound"
}

// returns a list of rancher host ids
func GetHostIdsByProjectId(client *rancher.RancherClient, projectID string) map[string]int {
	hostIds := map[string]int{}

	// get a list of hosts in the environment
	hosts, err := client.Host.List(nil)
	if err != nil {
		log.Error(err)
	}

	for _, h := range hosts.Data {
		if h.AccountId == projectID && h.State == "active" {
			hostIds[h.Id] = 0
		}
	}

	return hostIds
}

// returns the health state of an environemnt
func GetEnvironmentHealthByName(client *rancher.RancherClient, name string) string {
	projects, err := client.Project.List(nil)
	if err != nil {
		log.Error("error getting projects list")
	}

	for _, p := range projects.Data {
		if p.Name == name {
			return p.HealthState
		}
	}

	log.Error("environment " + name + " not found")

	return "NotFound"
}

// returns the host id of a container
func GetContainerHost(client *rancher.RancherClient, containerId string) string {
	services, err := client.Container.ById(containerId)
	if err != nil {
		log.Error(err)
	}

	return services.HostId
}

// returns the environment name of self via metadata service
func GetMetadataEnvironmentName(rancherMetadataUrl string) string {
	url := rancherMetadataUrl + "/latest/self/stack/environment_name"
	log.Debug("attempting to fetch environment from " + url)

	resp, err := http.Get(url)
	if err != nil {
		log.Errorf("rancher metadata not available ", err)
	}
	body, _ := ioutil.ReadAll(resp.Body)

	return strings.Replace(string(body), "\"", "", -1)
}

// de-activates a rancher host
func DeactivateHostByName(client *rancher.RancherClient, hostName string) bool {
	// get a list of hosts
	hosts, err := client.Host.List(nil)
	if err != nil {
		log.Error(err)
	}

	for _, h := range hosts.Data {
		if h.Hostname == hostName {
			client.Host.ActionEvacuate(&h)
			return true
		}
	}

	return false
}

// evacuates a rancher host
func EvacuateHostByName(client *rancher.RancherClient, hostName string) bool {
	// get a list of hosts
	hosts, err := client.Host.List(nil)
	if err != nil {
		log.Error(err)
	}

	for _, h := range hosts.Data {
		if h.Hostname == hostName {
			client.Host.ActionEvacuate(&h)
			return true
		}
	}

	return false
}

// return list of hosts having specified host label(client *rancher.RancherClient, hostName string)
func ListHostsByHostLabel(client *rancher.RancherClient, projectId string, hostLabel string) []*rancher.Host{
	var hostList []*rancher.Host

	// get a list of hosts
	hosts, err := client.Host.List(nil)
	if err != nil {
		log.Error(err)
	}

	label := strings.Split(hostLabel, "=")

	for _, h := range hosts.Data {
		if h.AccountId == projectId && h.State == "active" {
			for k, v := range h.Labels {
				if k == label[0] && v == label[1] {
					log.Debugf("Found host %s with label %s", h.Hostname, hostLabel)
					hostList = append(hostList, &h)
				}
			}
		}
	}

	return hostList
}

func ListRancherServices(client *rancher.RancherClient, projectId string, filter *rancher.ListOpts) []*rancher.Service {
	var servicesList []*rancher.Service
	listOpts := baseListOpts()

	// merge in user-provided filter
	if err := mergo.MergeWithOverwrite(listOpts, filter); err != nil {
		log.Errorf("error while merging filters: %s", err)
	}
	log.Debug("using filter: ", listOpts)

	services, err := client.Service.List(listOpts)
	if err != nil {
		log.Error(err)
	}

	for k := range services.Data {
		servicesList = append(servicesList, &services.Data[k])
	}

	return servicesList
}

// gets a stack name by stack id
func GetStackNameById(client *rancher.RancherClient, stackId string) string {
	stack, err := client.Stack.ById(stackId)
	if err != nil {
		log.Error(err)
	}

	return stack.Name
}

func ListContainersByInstanceIds(client *rancher.RancherClient, instanceIds []string) []*rancher.Container {
	var containersList []*rancher.Container

	for _, v := range instanceIds {
		container, err := client.Container.ById(v)
		if err != nil {
			log.Error(err)
		}

		containersList = append(containersList, container)
	}

	return containersList
}

// gets a container by container id
func GetContainerById(client *rancher.RancherClient, containerId string) *rancher.Container {
	container, err := client.Container.ById(containerId)
	if err != nil {
		log.Error(err)
	}

	return container
}
