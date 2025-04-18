---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/singleton-cli.html
---

# Singleton Command Line Interface [singleton-cli]

The `curator_cli` command allows users to run a single, supported action from the command-line, without needing either the client or action YAML configuration file, though it does support using the client configuration file if you want. As an important bonus, the command-line options allow you to override the settings in the `curator.yml` file!

::::{important}
While both the configuration file and the command-line arguments can be used together, it is important to note that command-line options will override file-based configuration of the same setting.
::::


```sh
$ curator_cli --help
Usage: curator_cli [OPTIONS] COMMAND [ARGS]...

  Curator CLI (Singleton Tool)

  Run a single action from the command-line.

  The default $HOME/.curator/curator.yml configuration file (--config) can be used but is not needed.

  Command-line settings will always override YAML configuration settings.

Options:
  --config PATH                   Path to configuration file.
  --hosts TEXT                    Elasticsearch URL to connect to.
  --cloud_id TEXT                 Elastic Cloud instance id
  --api_token TEXT                The base64 encoded API Key token
  --id TEXT                       API Key "id" value
  --api_key TEXT                  API Key "api_key" value
  --username TEXT                 Elasticsearch username
  --password TEXT                 Elasticsearch password
  --bearer_auth TEXT              Bearer authentication token
  --opaque_id TEXT                X-Opaque-Id HTTP header value
  --request_timeout FLOAT         Request timeout in seconds
  --http_compress / --no-http_compress
                                  Enable HTTP compression  [default: no-http_compress]
  --verify_certs / --no-verify_certs
                                  Verify SSL/TLS certificate(s)  [default: verify_certs]
  --ca_certs TEXT                 Path to CA certificate file or directory
  --client_cert TEXT              Path to client certificate file
  --client_key TEXT               Path to client key file
  --ssl_assert_hostname TEXT      Hostname or IP address to verify on the node's certificate.
  --ssl_assert_fingerprint TEXT   SHA-256 fingerprint of the node's certificate. If this value is given then root-of-trust
                                  verification isn't done and only the node's certificate fingerprint is verified.
  --ssl_version TEXT              Minimum acceptable TLS/SSL version
  --master-only / --no-master-only
                                  Only run if the single host provided is the elected master  [default: no-master-only]
  --skip_version_test / --no-skip_version_test
                                  Elasticsearch version compatibility check  [default: no-skip_version_test]
  --dry-run                       Do not perform any changes.
  --loglevel [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Log level
  --logfile TEXT                  Log file
  --logformat [default|ecs]       Log output format
  -v, --version                   Show the version and exit.
  -h, --help                      Show this message and exit.

Commands:
  alias             Add/Remove Indices to/from Alias
  allocation        Shard Routing Allocation
  close             Close Indices
  delete-indices    Delete Indices
  delete-snapshots  Delete Snapshots
  forcemerge        forceMerge Indices (reduce segment count)
  open              Open Indices
  replicas          Change Replica Count
  restore           Restore Indices
  rollover          Rollover Index associated with Alias
  show-indices      Show Indices
  show-snapshots    Show Snapshots
  shrink            Shrink Indices to --number_of_shards
  snapshot          Snapshot Indices

  Learn more at https://www.elastic.co/guide/en/elasticsearch/client/curator/8.0/singleton-cli.html
```

The option flags for the given commands match those used for the same [actions](/reference/actions.md).  The only difference is how filtering is handled.

## Running Curator from Docker [_running_curator_from_docker_2]

Running `curator_cli` from the command-line using Docker requires only a few additional steps.

Should you desire to use them, Docker-based `curator_cli` requires you to map a volume for your configuration and/or log files. Attempting to read a YAML configuration file if you have neglected to volume map your configuration directory to `/.curator` will not work.

It looks like this:

```sh
docker run [-t] --rm --name myimagename  \
  --entrypoint /curator/curator_cli      \
  -v /PATH/TO/MY/CONFIGS:/.curator       \
  untergeek/curator:mytag                \
  --config /.curator/config.yml [OPTIONS] COMMAND [ARGS]...
```

::::{note}
While testing, adding the `-t` flag will allocate a pseudo-tty, allowing you to see terminal output that would otherwise be hidden.
::::


The `config.yml` file should already exist in the path `/PATH/TO/MY/CONFIGS` before run time.

The `--rm` in the command means that the container (not the image) will be deleted after completing execution. You definitely want this as there is no reason to keep creating containers for each run. The eventual cleanup from this would be unpleasant.


## Command-line filtering [_command_line_filtering]

Recent improvements in Curator include schema and setting validation.  With these improvements, it is possible to validate filters and their many permutations if passed in a way that Curator can easily digest.

```sh
--filter_list TEXT  JSON string representing an array of filters.
```

This means that filters need to be passed as a single object, or an array of objects in JSON format.

Single:

```sh
--filter_list '{"filtertype":"none"}'
```

Multiple:

```sh
--filter_list '[{"filtertype":"age","source":"creation_date","direction":"older","unit":"days","unit_count":13},{"filtertype":"pattern","kind":"prefix","value":"logstash"}]'
```

This preserves the power of chained filters, making them available on the command line.

::::{note}
You may need to escape all of the double quotes on some platforms, or shells like PowerShell, for instance.
::::


Caveats to this approach:

1. Only one action can be taken at a time.
2. Not all actions have singleton analogs. For example, [Alias](/reference/alias.md) and<br> [Restore](/reference/restore.md) do not have singleton actions.


## Show Indices/Snapshots [_show_indicessnapshots]

One feature that the singleton command offers that the other cannot is to show which indices and snapshots are in the system.  It’s a great way to visually test your filters without causing any harm to the system.

```sh
$ curator_cli show-indices --help
Usage: curator_cli show-indices [OPTIONS]

  Show indices

Options:
  --verbose           Show verbose output.
  --header            Print header if --verbose
  --epoch             Print time as epoch if --verbose
  --filter_list TEXT  JSON string representing an array of filters.
                      [required]
  --help              Show this message and exit.

  Learn more at https://www.elastic.co/guide/en/elasticsearch/client/curator/8.0/singleton-cli.html#_show_indicessnapshots
```

```sh
$ curator_cli show-snapshots --help
Usage: curator_cli show-snapshots [OPTIONS]

  Show snapshots

Options:
  --repository TEXT   Snapshot repository name  [required]
  --filter_list TEXT  JSON string representing an array of filters.
                      [required]
  --help              Show this message and exit.

  Learn more at https://www.elastic.co/guide/en/elasticsearch/client/curator/8.0/singleton-cli.html#_show_indicessnapshots
```

The `show-snapshots` command will only show snapshots matching the provided filters.  The `show-indices` command will also do this, but also offers a few extra features.

* `--verbose` adds state, total size of primary and all replicas, the document count, the number of primary and replica shards, and the creation date in ISO8601 format.
* `--header` adds a header that shows the column names.  This only occurs if `--verbose` is also selected.
* `--epoch` changes the date format from ISO8601 to epoch time.  If `--header` is also selected, the column header title will change to `creation_date`

There are no extra columns or `--verbose` output for the `show-snapshots` command.

Without `--epoch`

```sh
Index               State     Size     Docs Pri Rep   Creation Timestamp
logstash-2016.10.20 close     0.0B        0   5   1 2016-10-20T00:00:03Z
logstash-2016.10.21  open  763.3MB  5860016   5   1 2016-10-21T00:00:03Z
logstash-2016.10.22  open  759.1MB  5858450   5   1 2016-10-22T00:00:04Z
logstash-2016.10.23  open  757.8MB  5857456   5   1 2016-10-23T00:00:04Z
logstash-2016.10.24  open  771.5MB  5859720   5   1 2016-10-24T00:00:00Z
logstash-2016.10.25  open  771.0MB  5860112   5   1 2016-10-25T00:00:01Z
logstash-2016.10.27  open  658.3MB  4872830   5   1 2016-10-27T00:00:03Z
logstash-2016.10.28  open  655.1MB  5237250   5   1 2016-10-28T00:00:00Z
```

With `--epoch`

```sh
Index               State     Size     Docs Pri Rep creation_date
logstash-2016.10.20 close     0.0B        0   5   1    1476921603
logstash-2016.10.21  open  763.3MB  5860016   5   1    1477008003
logstash-2016.10.22  open  759.1MB  5858450   5   1    1477094404
logstash-2016.10.23  open  757.8MB  5857456   5   1    1477180804
logstash-2016.10.24  open  771.5MB  5859720   5   1    1477267200
logstash-2016.10.25  open  771.0MB  5860112   5   1    1477353601
logstash-2016.10.27  open  658.3MB  4872830   5   1    1477526403
logstash-2016.10.28  open  655.1MB  5237250   5   1    1477612800
```

 


