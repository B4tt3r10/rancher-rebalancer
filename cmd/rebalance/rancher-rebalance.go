package main

import (
	"os"
	"time"

	"github.com/ScentreGroup/rancher-rebalancer/evencattle"
	r "github.com/ScentreGroup/rancher-rebalancer/rancher"
	log "github.com/Sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	VERSION   = "v0.0.0-dev"
	projectId string
)

func beforeApp(c *cli.Context) error {
	if c.GlobalBool("debug") {
		log.SetLevel(log.DebugLevel)
	}

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "rancher-rebalance"
	app.Version = VERSION
	app.Usage = "re-balance cattle container services"
	app.Action = start
	app.Before = beforeApp
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "rancher-url",
			Value:  "http://localhost:8080/",
			Usage:  "full url of the rancher server",
			EnvVar: "CATTLE_URL",
		},
		cli.StringFlag{
			Name:   "rancher-access-key",
			Usage:  "rancher access Key",
			EnvVar: "CATTLE_ACCESS_KEY",
		},
		cli.StringFlag{
			Name:   "rancher-secret-key",
			Usage:  "rancher secret Key",
			EnvVar: "CATTLE_SECRET_KEY",
		},
		cli.StringFlag{
			Name:   "rancher-environment",
			Usage:  "rancher environment",
			EnvVar: "CATTLE_ENVIRONMENT",
		},
		cli.StringFlag{
			Name:   "rancher-metadata-url",
			Value:  "http://rancher-metadata",
			Usage:  "base url of the rancher metadata service",
			EnvVar: "RANCHER_METADATA_URL",
		},
		// TODO: not yet implemented
		// we found that simply upgrading the service doesn't spread even
		// cli.StringFlag{
		// 	Name:   "strategy",
		// 	Value:  "condel",
		// 	Usage:  "the rebalancing strategy to use (condel or upgrade)",
		// 	EnvVar: "REBALANCE_STATEGY",
		// },
		cli.StringFlag{
			Name:   "label-filter",
			Usage:  "filter services by a label, default is rebalance=true",
			Value:  "rebalance=true",
			EnvVar: "REBALANCE_LABEL_FILTER",
		},
		cli.IntFlag{
			Name:   "poll-interval,t",
			Usage:  "polling interval in seconds",
			EnvVar: "POLL_INTERVAL",
			Value:  0,
		},
		// not yet implemented
		cli.BoolFlag{
			Name:  "dry",
			Usage: "run in dry mode",
		},
		cli.BoolFlag{
			Name:  "debug,d",
			Usage: "run in debug mode",
		},
	}
	app.Run(os.Args)
}

func start(c *cli.Context) error {
	if len(c.String("rancher-url")) < 1 {
		log.Errorf("no rancher url provided")
		os.Exit(1)
	}

	if len(c.String("rancher-access-key")) < 1 {
		log.Errorf("no rancher access key provided")
		os.Exit(1)
	}

	if len(c.String("rancher-secret-key")) < 1 {
		log.Errorf("no rancher secret key provided")
		os.Exit(1)
	}

	log.Info("start rancher rebalancer")

	rancherClient := r.CreateClient(c.String("rancher-url"),
		c.String("rancher-access-key"),
		c.String("rancher-secret-key"))

	// assume we should get the env that the container is running in
	environmentName := c.String("rancher-environment")
	if len(environmentName) < 1 {
		log.Debug("no specific environment provided, getting current environment from metadata server")

		// observed race condition where metadata is not yet updated immediately
		log.Debug("taking a quick snooze to allow metadata to be refreshed")
		time.Sleep(5 * time.Second)

		environmentName = r.GetMetadataEnvironmentName(c.String("rancher-metadata-url"))

		// TODO: if return is Not found - try again (metadata was still not up-to-date)
	}

	log.Infof("using the %s environment", environmentName)
	projectId = r.GetProjectIdByName(rancherClient, environmentName)
	log.Debug("project id is " + projectId)

	// start the health check server in a sub-process
	evencattle.StartHealthCheck()

	log.Info("entering main loop")

	if c.Int("poll-interval") > 0 {
		for {
			log.Debug("scan started at ", time.Now())
			evencattle.Rebalance(rancherClient, projectId, c.String("label-filter"))
			time.Sleep(time.Duration(c.Int("poll-interval")) * time.Second)
		}
	} else {
		evencattle.Rebalance(rancherClient, projectId, c.String("label-filter"))
	}

	return nil
}
