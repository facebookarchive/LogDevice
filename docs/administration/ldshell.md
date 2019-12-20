---
id: LDShell
title: LogDevice Shell (LDShell)
sidebar_label: LogDevice Shell
---
The LogDevice shell is the standard administrative tool for LogDevice
clusters, the tool offers two modes of operation, a CLI mode where you pass
the commands to `ldshell` as arguments to execute and exit. Another mode is the
_interactive_ mode where you start ldshell connected to a cluster without
passing a command, this will drop you into an interactive shell where you can
execute consecutive commands and get dynamic help and auto-completion of
commands, and arguments along the way.

LDShell is built on top of Facebook's
[python-nubia](https://github.com/facebookincubator/python-nubia.git) framework.
In order to use ldshell with a LogDevice cluster, you will need the location of
the configuration file handy. This can be a local config file or a zookeeper
path if you store the config file there. You can also start ldshell in a
"disconnected" mode by not supplying any arguments. This will start the
interactive mode where you can use the `connect` command to establish a
connection to a running cluster. The `connect` command needs to establish a
connection to the [admin server](administration/admin_server.md) 
(`ld-admin-server`) but can alternatively connect
to any node on the cluster via the admin port (default admin port for nodes is `6440`).

Alternatively, it can connect via a unix-socket to the admin server or admin
unix-socket of any running node.

![LDShell Screenshot](assets/ldshell-screenshot-1.png "LDShell Screenshot")

## Interactive Mode
The interactive mode starts automatically if no command was passed as an
argument to ldshell. The interactive mode can start in `DISCONNECTED` state if
no cluster information were passed (missing the `--admin-server-host/-s` argument).
In this case you will have to execute the `connect` command from within the
interactive mode. LDShell will print an example on how to do that upon startup.

In order to start LDShell (interactive) while connecting to a cluster, you need
to supply the admin server unix-socket path or IP/Hostname of the admin server.

```bash
# Assumes that admin server listens on TCP and default port (6440)
ldshell --admin-server-host localhost

# Short version
ldshell -s localhost

# If you want to supply a custom port
ldshell -s localhost --admin-server-port=6888

# In the case of unix-socket address of admin server
ldshell --admin-server-unix-path=/tmp/cluster/N0:1/socket_admin
```

Some of the benefits of using the interactive mode is that you have automatic
completion of commands, sub-command, and arguments. For instance, if you use
the [LogsConfig](Logs) commands ldshell will be helping out with auto-completion
and syntax highlighting. It will also suggest the arguments with interactive
contextual help.

A couple of handy tips, you can exit by `q`, `quit`, or `Ctrl+D`. Also use
`help` command to see which commands are available to you.

# CLI Mode

In this mode, ldshell will run given the command supplied and finish execution
to return the prompt back to the user. The exit code of LDShell should represent
whether the command failed or succeeded.
```shell-session
ldshell -s localhost logs show
```

# Using LDQuery from LDShell
LogDevice comes with a SQL-like query interface to query the internal state of
the various components of the system. This system is called [LDQuery](LDQuery)
  and the interface for using this is LDShell "query" command.

## LDQuery via CLI mode

```shell-session
ldshell -s localhost query "SELECT * FROM info"
```

## LDQuery via Interactive mode

```bash
ldshell -s localhost

ubuntu@my-cluster> SELECT * FROM info
...
```
