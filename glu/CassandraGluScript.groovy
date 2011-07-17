import org.linkedin.glu.agent.api.ShellExecException
import org.linkedin.util.io.resource.Resource

class CassandraGluScript {
  static requires = {
    agent(version: '1.6.0')
  }

  /*******************************************************
   * Script state
   *******************************************************/

  // the following fields represent the state of the script and will be exported to ZooKeeper
  // automatically thus will be available in the console or any other program 'listening' to
  // ZooKeeper

  // this 3.0.0 is replaced at build time
  def version = '0.8.1'
  def serverRoot
  def logsDir
  def serverLog
  def pid
  def pidFile

  /*******************************************************
   * install phase
   *******************************************************/

  // * log, shell and mountPoint are 3 'variables' available in any glu script
  // * note how we use 'mountPoint' for the jetty installation. It is done this way because the
  // agent automatically cleans up whatever goes in mountPoint on uninstall. Also mountPoint is
  // guaranteed to be unique so it is a natural location to install the software which allows
  // to install more than one instance of it on a given machine/agent.
  // * every file system call (going through shell.xx methods) is always relative to wherever
  // the agent apps folder was configured

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

  /*******************************************************
   * configure phase
   *******************************************************/

  // in this phase we set up a timer which will monitor the server. The reason why it is setup
  // in the configure phase rather than the start phase is because this way we can both detect
  // when the server goes down and up! (for example if you kill it on the command line and
  // restart it without going through glu, the monitor will detect it)

  def configure = {
    log.info "Configuring..."
    log.info "Configuration complete."
  }

  /*******************************************************
   * start phase
   *******************************************************/
  def start = {
    log.info "Starting..."
    pidFile = "${logsDir}/cassandra.pid"
    shell.exec("${serverRoot}/bin/cassandra -p ${pidFile}")
    // we wait for the process to be started (should be quick)
    shell.waitFor(timeout: '10s', heartbeat: '500') {
      pid = isProcessUp()
    }
  }

  /*******************************************************
   * stop phase
   *******************************************************/
  def stop = { args ->
    log.info "Stopping..."
    doStop()
    log.info "Stopped."
  }

  /*******************************************************
   * unconfigure phase
   *******************************************************/

  // we remove the timer set in the configure phase

  def unconfigure = {
    log.info "Unconfiguring..."
    log.info "Unconfiguration complete."
  }

  /*******************************************************
   * uninstall phase
   *******************************************************/

  // note that since it does nothing, it can simply be removed. It is there just to enforce the
  // fact that it really does nothing. Indeed the agent will automatically clean up after this
  // phase and delete whatever was installed under 'mountPoint'

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
