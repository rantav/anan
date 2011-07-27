import org.linkedin.glu.agent.api.ShellExecException
import org.linkedin.util.io.resource.Resource

class CassandraGluScript {
  static requires = {
    agent(version: '1.6.0')
  }

  // TODO: the version needs to be retained from teh tar.
  def version = '0.8.1'
  def serverRoot
  def logsDir
  def serverLog
  def pid
  def pidFile

  def install = {
    log.info "Installing..."

    serverRoot = downloadCassandra()

    // assigning variables
    logsDir = '/var/log/cassandra/'
    serverLog = logsDir + 'system.log'

    // make sure all bin/*.sh files are executable
    shell.ls(serverRoot.bin) {
      include(name: '*.sh')
      include(name: 'cassandra')
      include(name: 'cassandra-cli')
      include(name: 'json2sstable')
      include(name: 'nodetool')
      include(name: 'sstable2json')
      include(name: 'sstablekeys')
      include(name: 'sstableloader')
    }.each { shell.chmodPlusX(it) }

    log.info "Install complete."
  }

  private Resource downloadCassandra() {
    def cassandraSkeleton = shell.fetch(params.skeleton)
    def distribution = shell.untar(cassandraSkeleton)
    shell.rmdirs(mountPoint)
    def serverRoot = shell.mv(shell.ls(distribution)[0], mountPoint)
    return serverRoot
  }

  def configure = {
    log.info "Configuring..."
    shell.mkdirs(logsDir)
    log.info "Configuration complete."
  }

  def start = {
    log.info "Starting..."
    pidFile = "${logsDir}/cassandra.pid"
    shell.exec("${serverRoot}/bin/cassandra -p ${pidFile}")
    // we wait for the process to be started (should be quick)
    shell.waitFor(timeout: '10s', heartbeat: '500') {
      pid = isProcessUp()
    }
  }

  def stop = { args ->
    log.info "Stopping..."
    doStop()
    log.info "Stopped."
  }

  def unconfigure = {
    log.info "Unconfiguring..."
    // nothing
    log.info "Unconfiguration complete."
  }

  def uninstall = {
    // nothing
  }

  // a closure called by the rest of the code but not by the agent directly
  private def doStop = {
    if(isProcessDown()) {
      log.info "Cassandra is already down."
    } else {
      // invoke the stop command
      shell.exec("kill -9 ${pid}")

      // we wait for the process to be stopped
      shell.waitFor(timeout: params.stopTimeout, heartbeat: '1s') { duration ->
        log.info "${duration}: Waiting for server to be down"
        isProcessDown()
      }
    }
    pid = null
  }

  // a method called by the rest of the code but not by the agent directly

  // why use closure vs method ? the rule is simple: if you are modifying any field (the ones
  // defined at the top of this file), then use a closure otherwise the update won't make it to
  // ZooKeeper.

  private Integer isProcessUp() {
    try {
      def output = shell.exec("${serverRoot}/bin/nodetool -h localhost ring")
      def pid = shell.cat(pidFile)
      if (!shell.listening('localhost', 9160)) {
        log.info("Cassandra not yet listening on port 9160")
        return null
      }
      return pid as int
    } catch(ShellExecException e) {
      log.info("Not ready yet, gets an exception ${e.localizedMessage}")
      return null
    }
  }

  private boolean isProcessDown() {
    isProcessUp() == null
  }
}
