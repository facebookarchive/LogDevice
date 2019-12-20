---
id: Kubernetes
title: Running on Kubernetes
sidebar_label: Running on Kubernetes
---

This document describes how to run LogDevice kubernetes using the specs that we provide. The document assumes basic previous kubernetes knowledge. By the end of this section, you'll have a fully running LogDevice cluster on kubernetes that's ready to receive reads/writes.

## 1. Building your Kubernetes Cluster

The first step is to have a running kubernetes cluster. You can use a managed cluster (provided by your cloud provider), a self-hosted cluster or a local kubernetes cluster using a tool like [minikube](https://github.com/kubernetes/minikube). Make sure that `kubectl` points to whatever cluster you're planning to use.

## 2. Install Zookeeper

LogDevice depends on Zookeeper for storing per-log metadata and its nodes configuration. So we will need to provision a zookeeper ensemble that LogDevice will be able to access. For this demo, we will use [helm](https://helm.sh/) (A package manager for kubernetes) to install zookeeper. After installing helm run:

```shell-session
helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
helm repo update
helm install zookeeper incubator/zookeeper
```

```shell-session
NAME: zookeeper
LAST DEPLOYED: Fri Dec 20 14:19:41 2019
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Thank you for installing ZooKeeper on your Kubernetes cluster. More information
about ZooKeeper can be found at https://zookeeper.apache.org/doc/current/

Your connection string should look like:
  zookeeper-0.zookeeper-headless:2181,zookeeper-1.zookeeper-headless:2181,...

  You can also use the client service zookeeper:2181 to connect to an available ZooKeeper server.

```

> Take note of the printed zookeeper connection string, we will use it later on.

This will by default install a 3 nodes zookeeper ensemble. Wait until all the three pods are marked as ready:

```shell-session
kubectl get pods
```

```shell-session
NAME          READY   STATUS    RESTARTS   AGE
zookeeper-0   1/1     Running   1          2m20s
zookeeper-1   1/1     Running   0          99s
zookeeper-2   1/1     Running   0          74s
```

## 3. Configuring and Starting LogDevice

Once all the zookeeper pods are ready, we're ready to start installing the LogDevice cluster. 

### Fetching The K8s Specs

```shell-session
git clone https://github.com/facebookincubator/LogDevice.git
cd LogDevice/k8s
```

### Initial LogDevice Configuration

If you used a different way to install zookeeper, make sure to update the zookeeper connection string in the server config file `config.json`. It should look something like this:

```shell-session
$ cat config.json  | grep -A 3 zookeeper
  "zookeeper": {
    "zookeeper_uri": "ip://zookeeper-0.zookeeper-headless:2181,zookeeper-1.zookeeper-headless:2181,zookeeper-2.zookeeper-headless:2181",
    "timeout": "30s"
  }
}
```

By default, this spec installs a 3 nodes LogDevice cluster. If you want a bigger cluster, modify the `logdevice-statefulset.yaml` file, and increase the number of replicas to the number of nodes you want in the cluster. Also by default, we attach a 20GB persistent storage to the nodes, if you want more you can change that under the `volumeClaimTemplates` section.

Check out [our configuration docs](configuration.md) for more details about the configuration file.

### Starting the Cluster

Once you're done configuring the cluster, you can deploy it using:

```shell-session
$ kubectl apply -k .

configmap/logdevice-config created
configmap/nshards created
service/logdevice-admin-server-service created
service/logdevice created
deployment.apps/logdevice-admin-server-deployment created
statefulset.apps/logdevice created
```

This command will:
- Start all the nodes under a kubernetes StatefulSet.
- Start a standalone admin server to administrate the cluster.
- Store the config file in a ConfigMap and mount it to all the nodes.

When you run `kubectl get pods`, you should see something like this:

```shell-session
NAME                                                 READY   STATUS    RESTARTS   AGE
logdevice-0                                          1/1     Running   0          2m14s
logdevice-1                                          1/1     Running   0          2m2s
logdevice-2                                          1/1     Running   0          118s
logdevice-admin-server-deployment-6cdf46cd6c-d7gkp   1/1     Running   0          2m14s
zookeeper-0                                          1/1     Running   1          13m
zookeeper-1                                          1/1     Running   0          12m
zookeeper-2                                          1/1     Running   0          12m
```

> Tip: If you want to see the logs of one of the nodes run `kubectl logs logdevice-<id>`.

### Bootstrapping the Cluster

Once all the logdevice pods are running and ready, you'll need to bootstrap the cluster to enable all the nodes. To do that, run:

```shell-session
$ kubectl run ldshell -it --rm --restart=Never --image=facebookincubator/logdevice -- \
   ldshell --admin-server-host logdevice-admin-server-service \
   nodes-config \
   bootstrap --metadata-replicate-across 'NODE: 3'
```

This will start a ldshell pod, that connects to the admin server and invokes the `nodes-config bootstrap` ldshell command and sets the metadata replication property of the cluster to be replicated across three different nodes (more on metadata logs [here](configuration.md#metadata-logs-metadata-logs)). On success, you should see something like:

```shell-session
Logging to /tmp/ldshell-amfk8ock
Logging Level: WARNING
Successfully bootstrapped the cluster
pod "ldshell" deleted
```

## 4. Managing the Cluster

### LDshell

Now the cluster should be up and running and ready to receive reads and writes. To be able to get an interactive [ldshell](administration/ldshell.md) instance on this cluster run:

```shell-session
$ kubectl run ldshell -it --rm --restart=Never --image=facebookincubator/logdevice -- \
   ldshell --admin-server-host logdevice-admin-server-service
```

To check the state of the cluster, you can then run:

```shell-session
root@logdevice> status
ID  NAME         PACKAGE     STATE          UPTIME  LOCATION    SEQ.   DATA HEALTH  STORAGE STATE  SHARD OP.   HEALTH STATUS
0   logdevice-0  ?:99.99.99  ALIVE  13 minutes ago            ENABLED   HEALTHY(1)  READ_WRITE(1)  ENABLED(1)     HEALTHY
1   logdevice-1  ?:99.99.99  ALIVE  13 minutes ago            ENABLED   HEALTHY(1)  READ_WRITE(1)  ENABLED(1)     HEALTHY
2   logdevice-2  ?:99.99.99  ALIVE  13 minutes ago            ENABLED   HEALTHY(1)  READ_WRITE(1)  ENABLED(1)     HEALTHY
Took 9ms
```

### Changing Cluster Configs

If you want to edit the cluster config which is stored as a configmap, you can run:

```shell-session
kubectl edit configmaps logdevice-config
```

### Expanding the Cluster

If you want to add more nodes to the cluster you can either run:

```shell-session
kubectl scale statefulsets logdevice --replicas=<new_size>
statefulset.apps/logdevice scaled
```

Or you can modify the spec file, increase the number of replicas and then reapply it using `kubectl apply -k .`. You can then check if the pods are up by running `kubectl get pods`. Once the pods are started, they will auto register to the cluster and maintenance manager will enable them to be able to start serving traffic.

### Shrinking the Cluster

To be able to safely remove a node from a cluster, you first need to drain it (move all the data on this node, to other nodes). Check the [maintenance docs](adminstration/maintenances.md#draining-a-couple-of-nodes) to learn how to drain a node.

> Note: kubernetes only allows you to shrink nodes from the end, so the nodes with the highest IDs in their name are the ones that you should drain.

Once the drain is done (maintenance state is COMPLETED), decrease the number of replicas to the desired number using the same commands from the expand sections. Once the pod is deleted, you can then remove the node from the config using the `nodes-config shrink` ldshell command.
