---
layout: global
title: Running Spark on the cloud with Kubernetes
---

For general information about running Spark on Kubernetes, refer to [this section](running-on-kubernetes.md).

A Kubernetes cluster may be brought up on different cloud providers or on premise. It is commonly provisioned through [Google Container Engine](https://cloud.google.com/container-engine/), or using [kops](https://github.com/kubernetes/kops) on AWS, or on premise using [kubeadm](https://kubernetes.io/docs/getting-started-guides/kubeadm/).

## Running on Google Container Engine (GKE)

* Create a GKE [container cluster](https://cloud.google.com/container-engine/docs/clusters/operations).
* Find the name of the master associated with this project.

    > kubectl cluster-info
    Kubernetes master is running at https://x.y.z.w:443
* Run spark-submit with the master option set to `k8s://https://x.y.z.w:443`. The instructions for running spark-submit are provided in the [running on kubernetes](running-on-kubernetes.md) tutorial.
* Check that your driver pod, and subsequently your executor pods are launched using `kubectl get pods`.
* Read the stdout and stderr of the driver pod using `kubectl get logs`.

Known issues:
* If you face OAuth token expiry errors when you run spark-submit, it is likely because the token needs to be refreshed. The easiest way to fix this is to run any `kubectl` command, say, `kubectl version` and then retry your submission.



