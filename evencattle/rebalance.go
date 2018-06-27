package evencattle

import (
	"strings"
	"time"
	"bytes"
	"strconv"
	"io/ioutil"
	"net/http"

	r "github.com/ScentreGroup/rancher-rebalancer/rancher"
	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	rancher "github.com/rancher/go-rancher/v2"
)

type HostContainerCount struct {
	HostId       string
	Hostname     string
	Count        int
	ContainerIds []string
}

func NotifySlack(msg string) {
	var payload []byte

	payload = []byte(msg)
	req, err := http.NewRequest("POST", "https://kryten-siw.ops.scentregroup.io/", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	log.WithFields(log.Fields{
		"payload": string(payload),
		"headers": req.Header,
	}).Debug("request")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.WithFields(log.Fields{
		"status":  resp.Status,
		"headers": resp.Header,
		"body":    string(body),
	}).Debug("response")
}

func Rebalance(client *rancher.RancherClient, projectId string, labelFilter string, dryRun bool, slackChannel string) {
	var services []*rancher.Service

	// TODO: work out how to to filter modifier for scale>1
	filter := &rancher.ListOpts{
		Filters: map[string]interface{}{
			"accountId": projectId,
		},
	}

	collection := r.ListRancherServices(client, projectId, filter)
	log.Debug(len(collection), " initial services found")

	// if a label filter is provided, remove all services without that label
	if len(labelFilter) > 0 {
		log.Debugf("Finding services with %s label", labelFilter)
		label := strings.Split(labelFilter, "=")
		for _, s := range collection {
			for k, v := range s.LaunchConfig.Labels {
				if k == label[0] && v == label[1] {
					log.Debugf("Found service, '%s'", s.Name)
					services = append(services, s)
					break
				}
			}
		}
	} else {
		services = collection
	}

	// bail if there is nothing to balance
	if len(services) < 1 {
		log.Info("No candidate service to rebalance was found")
		return
	}

	// main services iteration
	for _, s := range services {
		excluded := false
		stackName := r.GetStackNameById(client, s.StackId)
		serviceRef := stackName + "/" + s.Name
		hostLabel := ""

		// reject an inactive service
		if s.State == "inactive" {
			log.Debugf("Skipping inactive service %s", serviceRef)
			excluded = true
		}

		// reject a service with a scale:1
		if s.Scale == 1 {
			log.Debugf("Skipping service %s whose scale is 1", serviceRef)
			excluded = true
		}

		// reject a global service
		for k, v := range s.LaunchConfig.Labels {
			if k == "io.rancher.scheduler.global" && v == "true" {
				log.Debugf("Skipping global service %s", serviceRef)
				excluded = true
			} else if k == "io.rancher.scheduler.affinity:host_label" {
				log.Debugf("%s has affinity host Label as %s", serviceRef, v.(string))
				hostLabel = v.(string)
			}
		}

		var spread []*HostContainerCount

		if excluded {
			log.Debugf("Service %s has been excluded", serviceRef)
		} else {
			// move onto balancing the service if not excluded

			containers := r.ListContainersByInstanceIds(client, s.InstanceIds)

			// algo to establish the spread of containers
			// iterate on each container
			for _, v := range containers {
				exists := false
				// if it doesn't exist add it
				for _, x := range spread {
					if x.HostId == v.HostId {
						x.Count = x.Count + 1
						// add the container id
						x.ContainerIds = append(x.ContainerIds, v.Id)
						exists = true
					}
				}
				if !exists {
					c := HostContainerCount{
						HostId:       v.HostId,
						Hostname:		  v.Hostname,
						Count:        1,
						ContainerIds: []string{v.Id},
					}
					spread = append(spread, &c)
				}
			}
			log.Debug(spew.Sdump(spread))
		}

		// get number of hosts according to host label so
		// newly joined host(s) are also counted
		// it should never get his far if you didn't scale > 1
		numHosts := len(r.ListHostsByHostLabel(client, projectId, hostLabel))
		if numHosts == 0 {
			// means the service does not have an affinity host label
			numHosts = len(spread)
		}
		perHost := int(s.Scale) / int(numHosts)

		// this is to avoid endless rebalancing when s.Scale is an odd value
		offset := int(s.Scale) % int(numHosts)

		log.Debug("Number of hosts: ", numHosts)
		log.Debugf("Scale: %d, expected per host: %d", s.Scale, perHost)

		log.WithFields(log.Fields{
			"containers": s.InstanceIds,
			"host_count": numHosts,
			"scale": s.Scale,
			"expected_per_host": perHost,
		}).Infof("Start to check %s", serviceRef)

		// iterate over each host in spread
		for _, m := range spread {
			if m.Count > int(perHost) {
				toDeleteCount := m.Count - int(perHost)
				if (offset != 0 && toDeleteCount == 1) {
					log.Info("No need to balance as total container number is odd")
					break;
				}

				// get the host by id and de-activate it
				host, err := client.Host.ById(m.HostId)
				if err != nil {
					log.Error(err)
					return
				}

				log.Debugf("Host %s is over-scheduled by %d containers", host.Hostname, toDeleteCount)

				// first, de-active the host
				if (dryRun) {
					log.Infof("Dry run mode, simulate to deactivate host %s", host.Hostname)
				} else {
					deactivation, err := client.Host.ActionDeactivate(host)
					if err != nil {
						log.Error(err, deactivation)
					}
					log.Debugf("Host %s deactivated", m.Hostname)
				}

				// second, kill the containers on the host
				// we only delete number of containers greater than desired number
				log.Infof("About to kill %d containers on %s", toDeleteCount, m.HostId)
				deleted := 0
				deletedContainerInfo := ""
				for _, containerId := range m.ContainerIds {
					if (dryRun) {
						log.Infof("Dry run mode, simulate to delete container %s", containerId)
					} else {
						log.Debugf("Deleting container %s ", containerId)
						container := r.GetContainerById(client, containerId)

						err := client.Container.Delete(container)
						if err != nil {
							log.Error(err)
						}

						deleted++

						if deletedContainerInfo != "" { deletedContainerInfo += "\n" }
						deletedContainerInfo += containerId + " | " + container.Name

						if deleted >= toDeleteCount { break }
					}
				}

				// a healthy snooze to allow re-scheduling to occur
				// multiple containers can be deleted at same time so required
				// delay time is not linear but 30s can be a best guess
				if (dryRun) {
					log.Infof("Dry run mode, simulate to wait...")
				} else {
					time.Sleep(30 * time.Second)
				}

				// third, re-active the host
				if (dryRun) {
					log.Infof("Dry run mode, simulate to activate host %s", host.Hostname)
				} else {
					host, err := client.Host.ById(m.HostId)
					if err != nil {
						log.Error(err)
					}

					activation, err := client.Host.ActionActivate(host)
					if err != nil {
						log.Error(err, activation)
					}
					log.Debugf("Host %s re-activated", host.Hostname)

					msg := "{\"channel\":\""+slackChannel+"\",\"username\":\"Rancher Rebalancer\", \"attachments\": [{\"color\":\"good\",\"fields\":[{\"title\":\"Unbalanced Service\",\"value\":\""+serviceRef+"\",\"short\":\"true\"},{\"title\":\"Unbalanced Host\", \"value\":\""+host.Hostname+"\",\"short\":\"true\"},{\"title\":\"Service Scale\", \"value\":\""+ strconv.Itoa(int(s.Scale)) +"\",\"short\":\"true\"},{\"title\":\"Total Host Number\", \"value\":\""+ strconv.Itoa(numHosts) +"\",\"short\":\"true\"},{\"title\":\"Action Performed\",\"value\": \""+ strconv.Itoa(toDeleteCount) +" container(s) have been rescheduled to other host(s)\"},{\"title\":\"Deleted Containers\",\"value\": \"" + deletedContainerInfo + "\"}]}]}"

					NotifySlack(msg)
				}
			}
		}
		log.Infof("Finished checking %s", serviceRef)
	}
}
