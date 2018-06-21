package evencattle

import (
	"strings"
	"time"

	r "github.com/ScentreGroup/rancher-rebalancer/rancher"
	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	rancher "github.com/rancher/go-rancher/v2"
)

type HostContainerCount struct {
	HostId       string
	Count        int
	ContainerIds []string
}

func Rebalance(client *rancher.RancherClient, projectId string, labelFilter string) {
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
		log.Debugf("filter out services without %s label", labelFilter)
		label := strings.Split(labelFilter, "=")
		for _, s := range collection {
			for k, v := range s.LaunchConfig.Labels {
				if k == label[0] && v == label[1] {
					log.Debugf("including service, '%s'", s.Name)
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
		log.Info("no candidate services to rebalance found")
		return
	}

	log.WithFields(log.Fields{
		"candidate_count": len(services),
	}).Info("rebalance services")

	// main services iteration
	for _, s := range services {
		excluded := false
		stackName := r.GetStackNameById(client, s.StackId)
		serviceRef := stackName + "/" + s.Name

		// reject an inactive service
		if s.State == "inactive" {
			log.Debugf("skipping service, %s due to being inactive", serviceRef)
			excluded = true
		}

		// reject a global service
		for k, v := range s.LaunchConfig.Labels {
			if k == "io.rancher.scheduler.global" && v == "true" {
				log.Debugf("skipping global service %s", serviceRef)
				excluded = true
			}
		}

		// reject a service with a scale:1
		if s.Scale == 1 {
			log.Debugf("skipping service, %s due to scale:1", serviceRef)
			excluded = true
		}

		var spread []*HostContainerCount

		if excluded {
			log.Debugf("service %s has been excluded", serviceRef)
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
						Count:        1,
						ContainerIds: []string{v.Id},
					}
					spread = append(spread, &c)
				}
			}
			log.Debug(spew.Sdump(spread))
		}

		// get number of hosts in play
		// it should never get his far if you didn't scale > 1
		numHosts := len(spread)
		perHost := s.Scale / int64(numHosts)
		log.Debug("number of hosts: ", numHosts)
		log.Debugf("scale: %s, expected per host, %s", s.Scale, perHost)

		log.WithFields(log.Fields{
			"containers": s.InstanceIds,
			"host_count": numHosts,
			"scale": s.Scale,
			"expected_per_host": perHost,
		}).Infof("balance %s", serviceRef)

		// iterate over each host in spread
		for _, m := range spread {
			if m.Count > int(perHost) {
				toDeleteCount := m.Count - int(perHost)
				log.Debugf("the host, %s appears to be over-scheduled by %d containers", m.HostId, toDeleteCount)

				// get the host by id and de-activate it
				host, err := client.Host.ById(m.HostId)
				if err != nil {
					log.Error(err)
				}

				// first, de-active the host
				deactivation, err := client.Host.ActionDeactivate(host)
				if err != nil {
					log.Error(err, deactivation)
				}
				log.Debugf("host %s deactivated", m.HostId)

				// second, kill the containers on the host
				// we only delete number of containers greater than desired number
				log.Debug("kill ", toDeleteCount, " containers on ", m.HostId)
				deleted := 0
				for _, containerId := range m.ContainerIds {
					log.Debugf("delete container %s ", containerId)
					container := r.GetContainerById(client, containerId)
					err := client.Container.Delete(container)
					if err != nil {
						log.Error(err)
					}

					deleted++
					if deleted >= toDeleteCount {
						break
					}
				}

				// a healthy snooze to allow re-scheduling to occur
				time.Sleep(time.Duration(int(toDeleteCount)) * 5 * time.Second)

				// third, re-active the host
				// get the host by id and de-activate it
				host, err = client.Host.ById(m.HostId)
				if err != nil {
					log.Error(err)
				}
				activation, err := client.Host.ActionActivate(host)
				if err != nil {
					log.Error(err, activation)
				}
				log.Debugf("host %s re-activated", m.HostId)
			}
		}
	}
}
