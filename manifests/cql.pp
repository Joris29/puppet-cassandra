# @summary Class to manage database cql resources.
#   Please note that Python is expected to be installed.
#
# @param connection_tries
#   How many times to try a connection to Cassandra. Also see `connection_try_sleep`.
# @param connection_try_sleep
#   How much time to allow between the number of tries specified in `connection_tries`.
# @param cqlsh_additional_options
#   Any additional options to be passed to the `cqlsh` command.
# @param cqlsh_client_config
#   Set this to a file name (e.g. '/root/.puppetcqlshrc')
#   This will contain the credentials for connecting to Cassandra.
# @param cqlsh_client_tmpl
#   The location of the template for configuring the credentials for the cqlsh client.
# @param cqlsh_command
#   The full path to the `cqlsh` command.
# @param cqlsh_host
#   The host for the `cqlsh` command to connect to.
# @param cqlsh_port
#   The port for the `cqlsh` command to connect to.
# @param cqlsh_user
#   The user for the cqlsh connection.
# @param cqlsh_password
#   The password for the cqlsh connection.
# @param cql_types
#   Creates new `cassandra::cql::cql_type` resources.
# @param indexes
#   Creates new `cassandra::cql::index` resources.
# @param keyspaces
#   Creates new `cassandra::cql::keyspace` resources.
# @param permissions
#   Creates new `cassandra::cql::permission` resources.
# @param tables
#   Creates new `cassandra::cql::table` resources.
# @param users
#   Creates new `cassandra::cql::user` resources.
#
class cassandra::cql (
  Integer $connection_tries = 6,
  Integer $connection_try_sleep = 30,
  Optional[String[1]] $cqlsh_additional_options = undef,
  Optional[Stdlib::Absolutepath] $cqlsh_client_config = undef,
  String[1] $cqlsh_client_tmpl = 'cassandra/cqlshrc.epp',
  Stdlib::Absolutepath $cqlsh_command = '/usr/bin/cqlsh',
  Variant[Stdlib::Host, Enum['localhost']] $cqlsh_host = 'localhost',
  Integer $cqlsh_port = 9042,
  String[1] $cqlsh_user = 'cassandra',
  Optional[Variant[String[1], Sensitive]] $cqlsh_password = undef,
  Hash $cql_types = {},
  Hash $indexes = {},
  Hash $keyspaces = {},
  Hash $permissions = {},
  Hash $tables = {},
  Hash $users = {},
) {
  require cassandra

  if $cqlsh_client_config {
    file { $cqlsh_client_config :
      ensure  => file,
      group   => $facts['identity']['gid'],
      mode    => '0600',
      owner   => $facts['identity']['uid'],
      content => epp($cqlsh_client_tmpl, { cqlsh_user => $cqlsh_user, cqlsh_password => $cqlsh_password }),
      before  => Exec['cassandra::cql connection test'],
    }

    $cmdline_login = "--cqlshrc=${cqlsh_client_config}"
  } else {
    if $cqlsh_password {
      warning('You may want to consider using the cqlsh_client_config attribute')
      $cmdline_login = "-u ${cqlsh_user} -p ${cqlsh_password}"
    } else {
      $cmdline_login = ''
    }
  }

  $cqlsh_opts = "${cqlsh_command} ${cmdline_login} ${cqlsh_additional_options}"
  $cqlsh_conn = "${cqlsh_host} ${cqlsh_port}"

  $connection_test = "${cqlsh_opts} -e 'DESC KEYSPACES' ${cqlsh_conn}"

  exec { 'cassandra::cql connection test':
    command   => $connection_test,
    returns   => 0,
    tries     => $connection_tries,
    try_sleep => $connection_try_sleep,
    unless    => $connection_test,
  }

  # manage keyspaces if present
  if $keyspaces {
    create_resources('cassandra::cql::keyspace', $keyspaces)
  }

  # manage cql_types if present
  if $cql_types {
    create_resources('cassandra::cql::cql_type', $cql_types)
  }

  # manage tables if present
  if $tables {
    create_resources('cassandra::cql::table', $tables)
  }

  # manage indexes if present
  if $indexes {
    create_resources('cassandra::cql::index', $indexes)
  }

  # manage users if present
  if $users {
    create_resources('cassandra::cql::user', $users)
  }

  # manage permissions if present
  if $permissions {
    create_resources('cassandra::cql::permission', $permissions)
  }

  # Resource Ordering
  Cassandra::Cql::Keyspace <| |> -> Cassandra::Cql::Cql_type <| |>
  Cassandra::Cql::Keyspace <| |> -> Cassandra::Cql::Table <| |>
  Cassandra::Cql::Keyspace <| |> -> Cassandra::Cql::Permission <| |>
  Cassandra::Cql::Cql_type <| |> -> Cassandra::Cql::Table <| |>
  Cassandra::Cql::Table <| |> -> Cassandra::Cql::Index <| |>
  Cassandra::Cql::Table <| |> -> Cassandra::Cql::Permission <| |>
  Cassandra::Cql::Index <| |> -> Cassandra::Cql::User <| |>
  Cassandra::Cql::User <| |> -> Cassandra::Cql::Permission <| |>
}
